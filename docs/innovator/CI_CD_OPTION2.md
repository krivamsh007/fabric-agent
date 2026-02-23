# Option 2 CI/CD (PR approvals + Deployment Pipelines)

This project supports a **real enterprise workflow**:

1. Make a change in the **Dev** workspace (e.g., SafeRefactor plan/apply).
2. Generate a **Change Packet** (a folder with `plan.json`, `review.html`, `deployment_request.json`).
3. Commit the packet to a branch and open a **PR**.
4. Reviewers approve in the PR (branch protection / required reviewers).
5. After merge, **CI** reads `deployment_request.json` and triggers a **Fabric Deployment Pipeline deploy** (Dev → Test, or Test → Prod).

Why this matters: approvals and audit live in Git (PR history), while Fabric deployment pipelines handle promotion across stages.

---

## 1) Create a change packet

From an existing SafeRefactor plan:

```bash
fabric-agent cicd create-packet \
  --plan memory/plans/<operation_id>.json \
  --out-dir change_packets \
  --pipeline-id <DEPLOYMENT_PIPELINE_ID> \
  --source-stage-id <DEV_STAGE_ID> \
  --target-stage-id <TEST_STAGE_ID> \
  --note "Rename measure [X]→[Y] - approved via PR"
```

This creates:

```
change_packets/<operation_id>/
  plan.json
  approval.json
  review.html
  deployment_request.json
  meta.json
```

---

## 2) Commit to a PR branch

```bash
fabric-agent cicd commit-packet \
  --packet-dir change_packets/<operation_id> \
  --push
```

Open a PR and require reviewers via branch protection rules.

---

## 3) Deploy in CI (after PR merge)

Use the built-in deploy command (recommended for GitHub Actions / Azure DevOps):

```bash
fabric-agent cicd deploy --request change_packets/<operation_id>/deployment_request.json --wait
```

The command:
- calls `POST /v1/deploymentPipelines/{id}/deploy`
- then polls `GET /v1/deploymentPipelines/{id}/operations/{deploymentId}` until Succeeded/Failed

---

## GitHub Actions (example)

This repo ships an example workflow:
- `.github/workflows/fabric_deploy.yml`

Recommended hardening:
- Use **GitHub Environments** (`fabric-test`, `fabric-prod`) with **required reviewers** for approval gates.
- Store secrets: `FABRIC_TENANT_ID`, `FABRIC_CLIENT_ID`, `FABRIC_CLIENT_SECRET`.

---

## Troubleshooting

- Ensure the caller has **Deployment Pipeline admin** role and is contributor on the stage workspaces.
- If deployment fails in CI with missing `deployment-id`, use the workflow-dispatch input to point to an explicit change packet.
- Service principal support depends on the item types involved; if you hit `PrincipalTypeNotSupported`, try delegated user auth or reduce item types.
