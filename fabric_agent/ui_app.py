from __future__ import annotations

"""Streamlit UI: Fabric Agent Control Tower.

This UI is intentionally:
- Human-in-the-loop (plan → review → approve → apply)
- Ticket/PR-friendly (generates static HTML review packets)

Run:
  pip install -e ".[ui]"
  streamlit run ui/app.py
"""

import json
from pathlib import Path
from typing import Any


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _list_json_files(dir_path: Path) -> list[Path]:
    if not dir_path.exists():
        return []
    return sorted([p for p in dir_path.glob("*.json") if p.is_file()], key=lambda p: p.stat().st_mtime, reverse=True)


def _risk_badge(risk: str) -> str:
    r = (risk or "LOW").upper()
    if r == "HIGH":
        return "🔴 HIGH"
    if r == "MEDIUM":
        return "🟠 MEDIUM"
    return "🟢 LOW"


def main() -> None:
    import streamlit as st
    from streamlit.components.v1 import html as st_html

    from fabric_agent.core.approval import approve_plan_file
    from fabric_agent.reporting import render_safe_refactor_html, render_schema_drift_html
    from fabric_agent.schema.contracts import DataContract
    from fabric_agent.schema.drift import simulate_proposed_contract

    st.set_page_config(page_title="Fabric Agent Control Tower", layout="wide")

    st.title("Fabric Agent Control Tower")
    st.caption(
        "Human-in-the-loop AI agents for Microsoft Fabric: SafeRefactor (report-safe renames) and Schema Drift management. "
        "This UI generates review packets you can attach to tickets/PRs and enforces approval gates."
    )

    root = Path.cwd()
    plans_dir = root / "memory" / "plans"
    ops_dir = root / "memory" / "operations"
    reviews_dir = root / "memory" / "reviews"
    reviews_dir.mkdir(parents=True, exist_ok=True)

    mode = st.sidebar.radio(
        "Mode",
        ["SafeRefactor (Plan/Review)", "Schema Drift (Detect/Review)", "Operations Viewer"],
        index=0,
    )

    # -------------------------
    # SafeRefactor
    # -------------------------
    if mode.startswith("SafeRefactor"):
        st.subheader("SafeRefactor: report-safe measure rename")
        st.write(
            "Load a SafeRefactor plan and generate a static HTML review packet (diffs + risk + approval command). "
            "If your plan contains `artifacts_dir`, diffs are shown directly in the UI."
        )

        plan_files = _list_json_files(plans_dir)
        col_a, col_b = st.columns([2, 1])
        with col_a:
            selected = st.selectbox(
                "Select an existing plan (memory/plans)",
                ["(none)"] + [p.name for p in plan_files],
                index=0,
            )
        with col_b:
            uploaded = st.file_uploader("…or upload plan.json", type=["json"], accept_multiple_files=False)

        plan_doc: dict[str, Any] | None = None
        plan_path: Path | None = None

        if selected != "(none)":
            plan_path = plans_dir / selected
            plan_doc = _read_json(plan_path)
        elif uploaded is not None:
            plan_doc = json.loads(uploaded.getvalue().decode("utf-8"))

        if not plan_doc:
            st.info("Select or upload a plan to continue.")
            return

        impact = plan_doc.get("impact", {})
        risk = str(impact.get("risk", "LOW"))
        st.success(f"Risk: {_risk_badge(risk)}")

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Operation", str(plan_doc.get("operation_id", "")))
        c2.metric("Model parts changed", str(len(impact.get("semantic_model_parts_changed", []) or [])))
        c3.metric("Reports changed", str(len(impact.get("reports_changed", {}) or {})))
        c4.metric("Old → New", f"{plan_doc.get('old')} → {plan_doc.get('new')}")

        with st.expander("Show full plan JSON", expanded=False):
            st.json(plan_doc)

        # Approvals
        st.markdown("### Approval")
        approver = st.text_input("Approver name", value="")
        comment = st.text_input("Comment (optional)", value="")

        approve_col1, approve_col2 = st.columns([1, 3])
        with approve_col1:
            do_approve = st.button("Approve plan", type="primary")
        with approve_col2:
            st.caption(
                "Approval is persisted to the plan file and a separate record under memory/approvals/. "
                "If you uploaded a plan (not selected from disk), you can download the approved JSON."
            )

        if do_approve:
            if not approver.strip():
                st.error("Approver name is required.")
            else:
                if plan_path is not None:
                    updated = approve_plan_file(str(plan_path), approver=approver.strip(), comment=comment or None)
                    st.success("Plan approved and written to disk.")
                    plan_doc = updated
                else:
                    # uploaded plan: approve in-memory and offer download
                    approval = plan_doc.get("approval") or {}
                    approval.update(
                        {
                            "status": "APPROVED",
                            "approved_by": approver.strip(),
                            "comment": comment or None,
                            "approved_ts": 0,
                        }
                    )
                    plan_doc["approval"] = approval
                    st.success("Plan approved (in-memory). Download the updated JSON below.")

        if plan_path is None:
            st.download_button(
                "Download current plan JSON",
                data=json.dumps(plan_doc, indent=2).encode("utf-8"),
                file_name=f"{plan_doc.get('operation_id','plan')}.json",
                mime="application/json",
            )

        # Review packet generation
        st.markdown("### Review Packet")
        artifacts_dir = plan_doc.get("artifacts_dir")
        if artifacts_dir:
            st.caption(f"Artifacts directory: {artifacts_dir}")
        else:
            st.warning(
                "This plan has no artifacts_dir. To generate diffs, create the plan with the latest CLI (it writes artifacts). "
                "You can still apply via CLI with --html-report to generate a review packet."
            )

        gen_html = st.button("Generate / refresh HTML review packet")
        if gen_html:
            if not artifacts_dir:
                st.error("Missing artifacts_dir in plan. Recreate plan with latest CLI.")
            else:
                ad = Path(artifacts_dir)
                sm_before = _read_json(ad / "sm_before.json")
                sm_after = _read_json(ad / "sm_after.json")
                reports_before = _read_json(ad / "reports_before.json")
                reports_after = _read_json(ad / "reports_after.json")
                out_path = reviews_dir / f"saferefactor_{plan_doc.get('operation_id','op')}.html"
                out = render_safe_refactor_html(
                    plan_doc=plan_doc,
                    sm_before=sm_before,
                    sm_after=sm_after,
                    reports_before=reports_before,
                    reports_after=reports_after,
                    output_path=out_path.as_posix(),
                    plan_path=(plan_path.as_posix() if plan_path else None),
                )
                st.success(f"Review packet written: {out}")

        # Display existing html if present
        html_path = reviews_dir / f"saferefactor_{plan_doc.get('operation_id','op')}.html"
        if html_path.exists():
            st.markdown("#### Preview")
            st_html(html_path.read_text(encoding="utf-8"), height=820, scrolling=True)
            st.download_button(
                "Download HTML review packet",
                data=html_path.read_bytes(),
                file_name=html_path.name,
                mime="text/html",
            )

    # -------------------------
    # Schema Drift
    # -------------------------
    elif mode.startswith("Schema Drift"):
        st.subheader("Schema Drift Agent: data contracts + drift detection")
        st.write(
            "Detect schema drift between an observed schema (JSON) and a contract (YAML). "
            "Generates an approval-gated plan + HTML review packet."
        )

        col1, col2 = st.columns(2)
        with col1:
            contract_file = st.file_uploader("Upload contract YAML", type=["yaml", "yml"], accept_multiple_files=False)
        with col2:
            observed_file = st.file_uploader("Upload observed schema JSON", type=["json"], accept_multiple_files=False)

        if contract_file is None or observed_file is None:
            st.info("Upload both contract YAML and observed schema JSON to continue.")
            return

        tmp_contract = reviews_dir / "_tmp_contract.yaml"
        tmp_obs = reviews_dir / "_tmp_observed.json"
        tmp_contract.write_bytes(contract_file.getvalue())
        tmp_obs.write_bytes(observed_file.getvalue())

        from fabric_agent.schema.drift import build_plan
        plan = build_plan(tmp_contract.as_posix(), tmp_obs.as_posix(), output_path=None)

        finding = plan.get("finding", {})
        st.success(
            f"Drift detected: breaking={bool(finding.get('breaking'))} • {finding.get('summary','')}"
        )

        # Build HTML review packet in-memory
        import yaml

        contract_before = tmp_contract.read_text(encoding="utf-8")
        c = DataContract.load(tmp_contract.as_posix())
        proposed = simulate_proposed_contract(c, plan)
        proposed_dict = {
            "name": proposed.name,
            "version": proposed.version,
            "owner": proposed.owner,
            "columns": [col.__dict__ for col in proposed.columns],
        }
        contract_after = yaml.safe_dump(proposed_dict, sort_keys=False)

        out_path = reviews_dir / f"schema_drift_{plan.get('operation_id','op')}.html"
        out = render_schema_drift_html(
            plan=plan,
            contract_before_text=contract_before,
            contract_after_text=contract_after,
            output_path=out_path.as_posix(),
            plan_path=None,
        )

        st.markdown("### Review Packet")
        st_html(out_path.read_text(encoding="utf-8"), height=820, scrolling=True)
        st.download_button(
            "Download HTML review packet",
            data=out_path.read_bytes(),
            file_name=out_path.name,
            mime="text/html",
        )

        st.markdown("### Plan JSON")
        st.download_button(
            "Download plan.json",
            data=json.dumps(plan, indent=2).encode("utf-8"),
            file_name=f"{plan.get('operation_id','schema_plan')}.json",
            mime="application/json",
        )


    # -------------------------
    # Change Packets (Option 2)
    # -------------------------
    elif mode.startswith("Change Packets"):
        st.subheader("Change Packets: PR approval → CI deploy (Option 2)")
        st.write(
            "Generate a **PR-friendly change packet** from an existing SafeRefactor plan. "
            "Reviewers approve the packet via PR. After merge, CI reads `deployment_request.json` and triggers "
            "a Fabric deployment pipeline deployment."
        )

        from fabric_agent.core.config import FabricSettings
        from fabric_agent.cicd.change_packet import create_change_packet_from_plan

        settings = FabricSettings()

        packet_root = root / "change_packets"
        packet_root.mkdir(parents=True, exist_ok=True)

        plan_files = _list_json_files(plans_dir)
        col_a, col_b = st.columns([2, 1])
        with col_a:
            selected = st.selectbox(
                "Select an existing plan (memory/plans)",
                ["(none)"] + [p.name for p in plan_files],
                index=0,
            )
        with col_b:
            uploaded = st.file_uploader("…or upload plan.json", type=["json"], accept_multiple_files=False)

        plan_path: Path | None = None
        tmp_plan: Path | None = None
        if selected != "(none)":
            plan_path = plans_dir / selected
        elif uploaded is not None:
            tmp_plan = reviews_dir / "_tmp_plan.json"
            tmp_plan.write_bytes(uploaded.getvalue())
            plan_path = tmp_plan

        if not plan_path:
            st.info("Select or upload a plan to continue.")
            return

        st.markdown("### Deployment Pipeline Targets")
        c1, c2, c3 = st.columns(3)
        with c1:
            pipeline_id = st.text_input("Deployment Pipeline ID", value=str(settings.fabric_default_deployment_pipeline_id or ""))
        with c2:
            source_stage_id = st.text_input("Source Stage ID (Dev)", value=str(settings.fabric_default_source_stage_id or ""))
        with c3:
            target_stage_id = st.text_input("Target Stage ID (Test/Prod)", value=str(settings.fabric_default_target_stage_id or ""))

        note = st.text_input("Deployment note", value=f"Approved via PR (packet from {plan_path.name})")
        include_items = st.checkbox("Selective deployment (infer items from plan)", value=True)

        if st.button("Create Change Packet", type="primary"):
            if not pipeline_id or not source_stage_id or not target_stage_id:
                st.error("Pipeline ID + Source Stage ID + Target Stage ID are required.")
                return

            paths = create_change_packet_from_plan(
                plan_path,
                out_dir=packet_root,
                deployment_pipeline_id=pipeline_id,
                source_stage_id=source_stage_id,
                target_stage_id=target_stage_id,
                note=note,
                include_items=include_items,
            )

            st.success(f"Created: {paths.packet_dir.as_posix()}")

            st.markdown("### Packet Contents")
            st.download_button("Download plan.json", data=paths.plan_json.read_bytes(), file_name="plan.json", mime="application/json")
            st.download_button("Download approval.json", data=paths.approval_json.read_bytes(), file_name="approval.json", mime="application/json")
            st.download_button("Download deployment_request.json", data=paths.deployment_request_json.read_bytes(), file_name="deployment_request.json", mime="application/json")
            st.download_button("Download review.html", data=paths.review_html.read_bytes(), file_name="review.html", mime="text/html")

            # Zip the whole packet for convenience
            import io, zipfile
            buf = io.BytesIO()
            with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
                for p in paths.packet_dir.rglob("*"):
                    if p.is_file():
                        zf.write(p, arcname=str(p.relative_to(paths.packet_dir.parent)))
            buf.seek(0)
            st.download_button(
                "Download full change packet (zip)",
                data=buf.getvalue(),
                file_name=f"{paths.packet_dir.name}_change_packet.zip",
                mime="application/zip",
            )

            st.markdown("### Next Steps (PR Approval → CI Deploy)")
            st.code(
                "\n".join(
                    [
                        f"git checkout -b change/{paths.packet_dir.name}",
                        f"git add {paths.packet_dir.as_posix()}",
                        f'git commit -m "Add change packet {paths.packet_dir.name}"',
                        "git push -u origin HEAD",
                        "# Open a PR and require reviewers (branch protection)",
                        "# After merge, GitHub Actions can run: fabric-agent cicd deploy --request <packet>/deployment_request.json",
                    ]
                ),
                language="bash",
            )

    # -------------------------
    # Operations viewer
    # -------------------------
    else:
        st.subheader("Operations Viewer")
        st.write("Load an executed operation record from memory/operations and render a review packet.")
        op_files = _list_json_files(ops_dir)
        selected = st.selectbox("Select operation", ["(none)"] + [p.name for p in op_files], index=0)
        if selected == "(none)":
            st.info("No operation selected.")
            return

        op_path = ops_dir / selected
        record = _read_json(op_path)
        plan = record.get("plan") or {}

        st.markdown("### Summary")
        impact = plan.get("impact", {})
        st.success(f"Risk: {_risk_badge(str(impact.get('risk','LOW')))}")
        st.json({"operation_id": record.get("operation_id"), "old": plan.get("old"), "new": plan.get("new")})

        out_path = reviews_dir / f"operation_{record.get('operation_id','op')}.html"
        from fabric_agent.reporting import render_safe_refactor_html

        out = render_safe_refactor_html(
            plan_doc=plan,
            sm_before=(record.get("before", {}).get("semantic_model") or {}),
            sm_after=(record.get("after", {}).get("semantic_model") or {}),
            reports_before=(record.get("before", {}).get("reports") or {}),
            reports_after=(record.get("after", {}).get("reports") or {}),
            output_path=out_path.as_posix(),
            plan_path=None,
        )
        st.caption(f"Rendered: {out}")
        st_html(out_path.read_text(encoding="utf-8"), height=820, scrolling=True)
        st.download_button(
            "Download HTML review packet",
            data=out_path.read_bytes(),
            file_name=out_path.name,
            mime="text/html",
        )


if __name__ == "__main__":
    main()
