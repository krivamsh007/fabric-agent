# Control Tower UI (Streamlit)

The Streamlit UI provides a **human-in-the-loop** experience for reviewing and approving changes.

It is designed to complement the CLI workflows:

- **SafeRefactor:** Plan → Review Packet → Approve → Apply → Rollback
- **Schema Drift:** Detect → Review Packet → Approve → Apply

## Install

```bash
pip install -e ".[ui]"
```

## Run

From the repo root:

```bash
streamlit run ui/app.py
```

Or:

```bash
fabric-agent ui
```

## What the UI can do

### SafeRefactor (Plan/Review)
- Load a plan from `memory/plans/` or upload a JSON plan.
- Generate/preview the **HTML Review Packet** in-app.
- Approve a plan (writes approval into the plan and writes a record under `memory/approvals/`).

> Tip: plans created by `fabric-agent saferefactor plan` now write artifacts to `memory/artifacts/<operation_id>/` so diffs can be rendered without re-calling Fabric.

### Schema Drift (Detect/Review)
- Upload a contract YAML and an observed schema JSON.
- Generates a drift plan and an HTML review packet.
- Download the plan for approval/apply via CLI.

### Operations Viewer
- Select an executed operation log from `memory/operations/`.
- Render and download the HTML review packet.
