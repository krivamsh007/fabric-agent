# Schema Drift Agent

Detect schema drift against versioned YAML contracts, produce a review plan, and apply safe updates **only after human approval**.

## Workflow
1) **Detect**: compare observed schema to the contract
2) **Review**: generate an **HTML review packet** (diff + recommendations)
3) **Approve**: stamp approval into the plan JSON
4) **Apply**: update the contract (additive changes by default)

## Commands

### Detect + generate HTML review packet
```bash
fabric-agent schema-drift detect \
  --contract contracts/example_contract.yaml \
  --observed examples/observed_schema.json \
  --output memory/plans/schema_plan.json \
  --html-report memory/reviews/schema_drift_review.html
```

### Approve the plan
```bash
fabric-agent schema-drift approve --plan memory/plans/schema_plan.json --approver "YOUR_NAME" --comment "Reviewed"
```

### Apply the plan
```bash
fabric-agent schema-drift apply --plan memory/plans/schema_plan.json
```

## Policy defaults
- **Additive drift** (new columns): allowed after approval
- **Breaking drift** (removed columns, type changes, nullability changes): flagged as breaking and blocked by default

You can fork the policy logic for your org (e.g., require additional checks, approvals, or staged rollout paths).
