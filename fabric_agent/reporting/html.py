from __future__ import annotations

import datetime as dt
import html
from difflib import HtmlDiff
from pathlib import Path
from typing import Any, Iterable


CSS = """
:root{
  --bg:#0b1020;
  --panel:#0f1730;
  --panel2:#101b3a;
  --text:#eaf0ff;
  --muted:#b9c6ff;
  --good:#2ecc71;
  --warn:#f1c40f;
  --bad:#e74c3c;
  --accent:#7aa2ff;
  --border: rgba(255,255,255,0.12);
}
*{box-sizing:border-box}
body{margin:0;font-family:ui-sans-serif,system-ui,-apple-system,Segoe UI,Roboto,Arial; background:var(--bg); color:var(--text)}
a{color:var(--accent)}
.container{max-width:1100px;margin:0 auto;padding:20px}
.header{display:flex;align-items:flex-start;justify-content:space-between;gap:16px;margin-bottom:16px}
.h1{font-size:22px;font-weight:800;margin:0 0 4px 0}
.sub{color:var(--muted);font-size:13px}
.grid{display:grid;grid-template-columns:repeat(12,1fr);gap:12px}
.card{background:linear-gradient(180deg,var(--panel),var(--panel2));border:1px solid var(--border);border-radius:16px;padding:14px}
.kv{display:flex;justify-content:space-between;gap:12px;margin:8px 0}
.k{color:var(--muted);font-size:12px}
.v{font-size:13px;max-width:70%;text-align:right;word-break:break-word}
.badge{display:inline-flex;align-items:center;gap:6px;padding:6px 10px;border-radius:999px;border:1px solid var(--border);font-weight:700;font-size:12px}
.badge.low{background:rgba(46,204,113,0.12);color:var(--good)}
.badge.medium{background:rgba(241,196,15,0.12);color:var(--warn)}
.badge.high{background:rgba(231,76,60,0.12);color:var(--bad)}
.section{margin-top:14px}
.section h2{margin:0 0 10px 0;font-size:16px}
.mono{font-family:ui-monospace,SFMono-Regular,Menlo,Monaco,Consolas,"Liberation Mono","Courier New",monospace}
.small{font-size:12px;color:var(--muted)}
.btnrow{display:flex;gap:10px;flex-wrap:wrap}
.btn{cursor:pointer;user-select:none;background:rgba(122,162,255,0.14);color:var(--text);border:1px solid var(--border);padding:8px 10px;border-radius:12px;font-weight:700;font-size:12px}
.btn:hover{background:rgba(122,162,255,0.22)}
.details{border:1px solid var(--border);border-radius:14px;overflow:hidden;background:rgba(0,0,0,0.15)}
.details summary{cursor:pointer;padding:10px 12px;font-weight:800}
.details .inner{padding:10px 12px;border-top:1px solid var(--border)}

/* difflib.HtmlDiff */
.tablewrap{overflow:auto;border-radius:12px;border:1px solid var(--border)}
.diff{width:100%;border-collapse:collapse;font-size:12px}
.diff th{position:sticky;top:0;background:rgba(255,255,255,0.06);backdrop-filter: blur(8px)}
.diff td,.diff th{padding:4px 6px;border:1px solid rgba(255,255,255,0.08)}
.diff_add{background:rgba(46,204,113,0.18)}
.diff_sub{background:rgba(231,76,60,0.18)}
.diff_chg{background:rgba(241,196,15,0.18)}

.footer{margin-top:18px;color:var(--muted);font-size:12px}
.codebox{white-space:pre-wrap;background:rgba(0,0,0,0.25);border:1px solid var(--border);border-radius:12px;padding:10px}
"""

JS = """
function setAll(open){
  document.querySelectorAll('details').forEach(d=>{d.open=open});
}
function copyText(id){
  const el=document.getElementById(id);
  if(!el) return;
  const text=el.innerText;
  navigator.clipboard.writeText(text);
}
"""


def _badge(risk: str) -> str:
    r = (risk or "").strip().upper()
    cls = "low" if r == "LOW" else "medium" if r == "MEDIUM" else "high"
    return f'<span class="badge {cls}">RISK: {html.escape(r)}</span>'


def _kv(k: str, v: str) -> str:
    return f'<div class="kv"><div class="k">{html.escape(k)}</div><div class="v mono">{html.escape(v)}</div></div>'


def _diff_table(before: str, after: str, *, context: bool = True, numlines: int = 3) -> str:
    hd = HtmlDiff(tabsize=2, wrapcolumn=90)
    # HtmlDiff expects lists of lines
    b = (before or "").splitlines()
    a = (after or "").splitlines()
    tbl = hd.make_table(b, a, fromdesc="before", todesc="after", context=context, numlines=numlines)
    return f'<div class="tablewrap">{tbl}</div>'


def _html_page(title: str, body: str) -> str:
    return f"""<!doctype html>
<html>
<head>
  <meta charset=\"utf-8\" />
  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />
  <title>{html.escape(title)}</title>
  <style>{CSS}</style>
</head>
<body>
  <div class=\"container\">{body}</div>
  <script>{JS}</script>
</body>
</html>"""


def _now_iso() -> str:
    return dt.datetime.now(tz=dt.timezone.utc).isoformat(timespec="seconds")


def _render_change_list(title: str, items: Iterable[str]) -> str:
    li = "".join([f"<li class='mono'>{html.escape(x)}</li>" for x in items])
    if not li:
        li = "<li class='small'>None</li>"
    return f"<div class='section'><h2>{html.escape(title)}</h2><ul>{li}</ul></div>"


def render_safe_refactor_html(
    *,
    plan_doc: dict[str, Any],
    sm_before: dict[str, Any],
    sm_after: dict[str, Any],
    reports_before: dict[str, dict[str, Any]],
    reports_after: dict[str, dict[str, Any]],
    output_path: str,
    plan_path: str | None = None,
) -> str:
    """Create a visually reviewable HTML artifact for SafeRefactor plan.

    This is intentionally static HTML so it can be checked into tickets, attached to PRs,
    or shared in change-control workflows.
    """
    op_id = str(plan_doc.get("operation_id", ""))
    impact = plan_doc.get("impact", {})
    risk = str(impact.get("risk", "LOW"))
    old = str(plan_doc.get("old", ""))
    new = str(plan_doc.get("new", ""))

    plan_hint = plan_path or "<path-to-plan.json>"
    approve_cmd = f"fabric-agent saferefactor approve --plan {plan_hint} --approver \"YOUR_NAME\" --comment \"Approved after review\""

    header = f"""
    <div class='header'>
      <div>
        <div class='h1'>SafeRefactor Review Packet</div>
        <div class='sub'>Generated {_now_iso()} • Operation <span class='mono'>{html.escape(op_id)}</span></div>
      </div>
      <div>{_badge(risk)}</div>
    </div>
    """

    summary = [
        "<div class='grid'>",
        "  <div class='card' style='grid-column: span 6'>",
        "    <div class='section'><h2>Summary</h2>",
        _kv("Old → New", f"{old} → {new}"),
        _kv("Workspace", str(plan_doc.get("workspace_id", ""))),
        _kv("Semantic Model", str(plan_doc.get("semantic_model_id", ""))),
        _kv("Semantic model parts changed", str(len(impact.get("semantic_model_parts_changed", [])))),
        _kv("Reports changed", str(len(impact.get("reports_changed", {})))),
        "    </div>",
        "  </div>",
        "  <div class='card' style='grid-column: span 6'>",
        "    <div class='section'><h2>Human approval</h2>",
        "      <div class='small'>This tool is designed for enterprise change-control. Review diffs below, then approve.</div>",
        f"      <div class='codebox mono' id='approve_cmd'>{html.escape(approve_cmd)}</div>",
        "      <div class='btnrow' style='margin-top:10px'>",
        "        <div class='btn' onclick=\"copyText('approve_cmd')\">Copy approval command</div>",
        "        <div class='btn' onclick=\"setAll(true)\">Expand all diffs</div>",
        "        <div class='btn' onclick=\"setAll(false)\">Collapse all diffs</div>",
        "      </div>",
        "      <div class='small' style='margin-top:10px'>Approval record is written to the plan file and stored under <span class='mono'>memory/approvals/</span>.</div>",
        "    </div>",
        "  </div>",
        "</div>",
    ]

    # Notes
    notes = impact.get("notes") or []
    notes_html = "".join([f"<li>{html.escape(str(n))}</li>" for n in notes]) or "<li class='small'>None</li>"
    notes_block = f"<div class='card section'><h2>Notes</h2><ul>{notes_html}</ul></div>"

    # Diffs: semantic model
    diffs: list[str] = []
    sm_parts_changed = impact.get("semantic_model_parts_changed") or []
    if sm_parts_changed:
        diffs.append("<div class='section'><h2>Semantic model diffs</h2></div>")
        before_parts = {p.get("path"): p.get("payload") for p in sm_before.get("parts", [])}
        after_parts = {p.get("path"): p.get("payload") for p in sm_after.get("parts", [])}
        for path in sm_parts_changed:
            b64_b = before_parts.get(path)
            b64_a = after_parts.get(path)
            if not b64_b or not b64_a:
                continue
            import base64

            b_txt = base64.b64decode(b64_b).decode("utf-8", errors="replace")
            a_txt = base64.b64decode(b64_a).decode("utf-8", errors="replace")
            diffs.append(
                f"<details class='details' open><summary>SemanticModel: <span class='mono'>{html.escape(path)}</span></summary>"
                f"<div class='inner'>{_diff_table(b_txt, a_txt)}</div></details>"
            )

    # Diffs: reports
    reports_changed = impact.get("reports_changed") or {}
    if reports_changed:
        diffs.append("<div class='section'><h2>Report diffs</h2></div>")
        import base64

        for rid, paths in reports_changed.items():
            b = reports_before.get(rid, {})
            a = reports_after.get(rid, {})
            bparts = {p.get("path"): p.get("payload") for p in b.get("parts", [])}
            aparts = {p.get("path"): p.get("payload") for p in a.get("parts", [])}
            for path in paths:
                if path not in bparts or path not in aparts:
                    continue
                b_txt = base64.b64decode(bparts[path]).decode("utf-8", errors="replace")
                a_txt = base64.b64decode(aparts[path]).decode("utf-8", errors="replace")
                diffs.append(
                    f"<details class='details'><summary>Report {html.escape(str(rid))}: <span class='mono'>{html.escape(path)}</span></summary>"
                    f"<div class='inner'>{_diff_table(b_txt, a_txt)}</div></details>"
                )

    changes_list = "".join(
        [
            _render_change_list("Semantic model changed parts", sm_parts_changed),
            _render_change_list("Reports changed (IDs)", list(reports_changed.keys())),
        ]
    )

    body = header + "".join(summary) + notes_block + f"<div class='card section'>{changes_list}</div>" + "".join(diffs)
    body += "<div class='footer'>Generated by fabric-agent-innovator. Store this HTML with your change ticket for auditability.</div>"

    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(_html_page("SafeRefactor Review Packet", body), encoding="utf-8")
    return out.as_posix()


def render_schema_drift_html(
    *,
    plan: dict[str, Any],
    contract_before_text: str,
    contract_after_text: str,
    output_path: str,
    plan_path: str | None = None,
) -> str:
    """Create an HTML review packet for a schema drift plan."""
    risk = "HIGH" if plan.get("finding", {}).get("breaking") else "LOW"
    plan_hint = plan_path or "<path-to-plan.json>"
    approve_cmd = f"fabric-agent schema-drift approve --plan {plan_hint} --approver \"YOUR_NAME\" --comment \"Approved schema contract update\""

    header = f"""
    <div class='header'>
      <div>
        <div class='h1'>Schema Drift Review Packet</div>
        <div class='sub'>Generated {_now_iso()} • Contract <span class='mono'>{html.escape(str(plan.get('contract_name','')))}</span></div>
      </div>
      <div>{_badge(risk)}</div>
    </div>
    """

    finding = plan.get("finding", {})
    summary = [
        "<div class='grid'>",
        "  <div class='card' style='grid-column: span 6'>",
        "    <div class='section'><h2>Summary</h2>",
        _kv("Contract", str(plan.get("contract_path", ""))),
        _kv("From → Proposed version", f"{plan.get('from_version')} → {plan.get('proposed_version')}"),
        _kv("Breaking", str(bool(finding.get("breaking")))),
        _kv("Finding", str(finding.get("summary", ""))),
        "    </div>",
        "  </div>",
        "  <div class='card' style='grid-column: span 6'>",
        "    <div class='section'><h2>Human approval</h2>",
        "      <div class='small'>Review the contract diff below. Breaking changes are blocked by policy unless explicitly allowed.</div>",
        f"      <div class='codebox mono' id='approve_cmd2'>{html.escape(approve_cmd)}</div>",
        "      <div class='btnrow' style='margin-top:10px'>",
        "        <div class='btn' onclick=\"copyText('approve_cmd2')\">Copy approval command</div>",
        "        <div class='btn' onclick=\"setAll(true)\">Expand all</div>",
        "        <div class='btn' onclick=\"setAll(false)\">Collapse all</div>",
        "      </div>",
        "    </div>",
        "  </div>",
        "</div>",
    ]

    recs = plan.get("recommendations") or []
    rec_li = "".join(
        [
            f"<li><span class='badge {'high' if r.get('risk')=='HIGH' else 'medium' if r.get('risk')=='MEDIUM' else 'low'}'>"
            f"{html.escape(str(r.get('type','')))}</span> {html.escape(str(r.get('message','')))}</li>"
            for r in recs
        ]
    ) or "<li class='small'>None</li>"

    diff = _diff_table(contract_before_text, contract_after_text)

    details = (
        "<div class='card section'><h2>Recommendations</h2><ul>" + rec_li + "</ul></div>"
        "<details class='details' open><summary>Contract diff (before → proposed)</summary><div class='inner'>"
        + diff
        + "</div></details>"
    )

    body = header + "".join(summary) + details
    body += "<div class='footer'>Generated by fabric-agent-innovator. Keep this artifact with your governance records.</div>"

    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(_html_page("Schema Drift Review Packet", body), encoding="utf-8")
    return out.as_posix()
