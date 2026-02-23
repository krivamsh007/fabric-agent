# SafeRefactor: Report-safe Measure Rename (REST-only)

## Problem
Renaming measures can break downstream Power BI reports because visuals and report metadata can embed measure references.

## Approach
This agent updates both, transactionally:
- **Semantic model definition** (TMDL)
- **Report definition parts** (PBIR)

It uses Fabric REST **Definition APIs**:
- `getDefinition` to fetch full definitions (parts)
- `updateDefinition` to write full definitions (parts)

## Safety model (human-in-the-loop)
1) **Plan**: compute impact + risk and generate an **HTML review packet** with diffs
2) **Approve**: a human stamps approval into the plan JSON
3) **Apply**: updates **reports first**, then the semantic model
4) **Audit**: writes immutable operation record to `memory/operations/<operation_id>.json`
5) **Rollback**: replays the saved snapshots to restore the pre-change state

## Commands

### Plan + generate HTML review packet
```bash
fabric-agent saferefactor plan \
  --workspace-id <ws> --semantic-model-id <model> \
  --old "[Old Measure]" --new "[New Measure]" \
  --plan-out memory/plans/plan.json \
  --html-report memory/reviews/safe_refactor_review.html
```

### Approve the plan
```bash
fabric-agent saferefactor approve --plan memory/plans/plan.json --approver "YOUR_NAME" --comment "Reviewed diffs"
```

### Apply transactionally
```bash
fabric-agent saferefactor apply --plan memory/plans/plan.json
```

### Rollback
```bash
fabric-agent saferefactor rollback --operation-id <op_id>
```

### Generate an HTML review packet from an executed operation
```bash
fabric-agent saferefactor report --operation-id <op_id> --output memory/reviews/op_<op_id>.html
```

## Notes
- This implementation replaces the **exact bracketed token** (e.g., `[Sales]`). It will not rename substrings.
- Non-UTF parts are preserved byte-for-byte.
- For high-risk models, consider extending the parser to be format-aware (PBIR JSON and TMDL grammar).
