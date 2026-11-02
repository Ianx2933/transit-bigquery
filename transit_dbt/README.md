# transit_dbt

Transformation layer for transit-bigquery. Reads the 1:1 staging copy in
`transit_staging` and builds the dimensional model. See the repository
README for design decisions, grain and validation.

```bash
dbt build      # models + tests; fails if any month stops reconciling to staging
```
