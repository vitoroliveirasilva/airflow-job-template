# API -> database pattern

The executable example lives in:

- `dags/example_api_to_database.py`
- `src/airflow_job_template/jobs/example_api_to_database/job.py`

It deliberately resolves `example_crm_api` and `example_analytics_postgres` only while the Airflow task is running. Pagination returns records incrementally, writes are batched, and the PostgreSQL statement uses a unique key plus `ON CONFLICT ... DO UPDATE` so a whole-task retry does not create duplicate customers.

The example assumes the target has a table equivalent to:

```sql
CREATE TABLE customer_sync (
    external_id text PRIMARY KEY,
    name text NOT NULL,
    updated_at timestamptz NOT NULL
);
```

Do not copy these example connection IDs into a production job unless they represent the actual resources. Create Connections in Airflow/your Secrets Backend instead of putting credentials here.
