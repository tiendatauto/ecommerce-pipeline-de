# Simplified batch workflow

Configure the Databricks Workflow as one linear chain at the phase level:

```text
Identify Next Batch
        |
Create New Batch
        |
Run Ingestion Pipeline
        |
Run Transformation Pipeline
        |
Run Gold Pipeline
        |
Complete Batch
```

Use these notebooks for the two consolidated phase tasks:

- `02-bronze/00.Run Ingestion Pipeline`
- `03-silver/00.Run Transformation Pipeline`
- `04-gold/00.Run Gold Pipeline`

Set `p_batch_id` on `Create New Batch`, all three phase tasks, and
`Complete Batch` to the `p_batch_id` task value produced by
`Identify Next Batch`.

The ingestion runner owns the internal dependency order. It ingests the four API
datasets first, then extracts cart items and product reviews from their parent
Bronze datasets. The transformation runner invokes all six Silver transforms.
The Gold runner builds dimensions before the sales and product-review facts.
Any child failure raises through `dbutils.notebook.run`, so the phase task fails
and the workflow does not advance to the next phase.

`91.Build Inventory Status Reference` contains static reference data and should
run during environment setup, not on every batch.
