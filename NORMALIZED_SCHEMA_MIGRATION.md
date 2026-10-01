# Additive normalized-schema migration

This migration is **opt-in**. It does not run during FastAPI startup, does not alter current API queries, and does not drop, rename, or modify legacy tables.

It creates a parallel `fc_` namespace based on `DATABASE_DESIGN_PROPOSAL.md`. The `fc_` prefix is deliberate because the existing database already owns names such as `batches`, `users`, and `sensor_data`.

## What it creates

The migration creates normalized foundation tables for:

- organizations, roles, users, and user roles;
- products, units, locations, batches, and batch identifiers;
- devices, device assignments, ingestion messages, sensor readings, and sensor health;
- supply-chain stages, transport orders, batch events, and current-state projections;
- alert rules, alerts, and notifications;
- integrity records and Fabric transactions;
- encrypted-record metadata, audit logs, and reconciliation results.

The existing tables remain untouched and remain the source used by current dashboards/APIs.

## Safety rules

1. Run against a copy first.
2. Stop the backend and MQTT publisher before the production run so the backup and source database are stable.
3. Always pass `--apply` deliberately.
4. Always create and verify a backup.
5. Inspect `fc_reconciliation_checks` before considering the result usable.
6. Do not point current APIs at `fc_` tables yet; this migration only establishes a foundation.

## Dry plan

From the repository root:

```powershell
python backend\migrate_normalized_schema.py --db backend\food_chain.db
```

This performs no writes.

## Recommended test on a copied database

```powershell
Copy-Item backend\food_chain.db backend\food_chain.normalized-test.db
python backend\migrate_normalized_schema.py `
  --db backend\food_chain.normalized-test.db `
  --backup backend\food_chain.normalized-test.before.db `
  --apply
```

Inspect the result:

```powershell
python -c "import sqlite3; c=sqlite3.connect('backend/food_chain.normalized-test.db'); print(c.execute(\"SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'fc_%' ORDER BY name\").fetchall()); print(c.execute('SELECT * FROM fc_reconciliation_checks').fetchall())"
```

The migration is idempotent for copied data: rerunning it uses `IF NOT EXISTS` and legacy-source uniqueness keys rather than duplicating normalized rows. Reconciliation rows are recorded per run.

## Production run

1. Stop `main.py`, MQTT publishers, and any process writing `backend\food_chain.db`.
2. Copy the database to a dated backup:

```powershell
Copy-Item backend\food_chain.db "backups\food_chain.before-normalized-$(Get-Date -Format yyyyMMdd-HHmmss).db"
```

3. Run the explicit migration:

```powershell
python backend\migrate_normalized_schema.py `
  --db backend\food_chain.db `
  --backup "backups\food_chain.migration-source-$(Get-Date -Format yyyyMMdd-HHmmss).db" `
  --apply
```

4. Check the latest run:

```sql
SELECT * FROM fc_migration_runs ORDER BY run_id DESC LIMIT 1;
SELECT source_table, check_name, source_count, migrated_count,
       skipped_count, status, details_json
FROM fc_reconciliation_checks
WHERE run_id = (SELECT MAX(run_id) FROM fc_migration_runs)
ORDER BY check_id;
```

5. Confirm legacy tables and endpoints still work before restarting normal traffic.

## What “review” means

`pass` means the migration mapped the deterministic source rows for that check. `review` means some source rows were intentionally not copied because the batch/device relationship could not be proven safely. A review result is not silently repaired.

Examples of safe review items:

- sensor row has no batch/product key;
- legacy role is not in the canonical role map;
- quantity text has no parseable number/unit;
- sensor row references a batch that does not exist and cannot be safely inferred.

## Rollback

Because the migration is additive, the safest rollback is operational:

1. Stop the backend and writers.
2. Leave the legacy database tables and APIs unchanged.
3. Restore the pre-migration backup only if the database file itself must be reverted:

```powershell
Copy-Item "backups\food_chain.before-normalized-YYYYMMDD-HHMMSS.db" backend\food_chain.db -Force
```

Do not manually drop individual `fc_` tables in a production database. If the migration was run only on a copy, delete that copy. If a future cutover is implemented, it must have its own separately tested rollback plan.

## Explicit non-goals

- No current dashboard/API reads are changed.
- No automatic startup migration is added.
- No sensor, GPS, hash, or Fabric value is invented.
- No legacy table is deleted, renamed, or altered.
- No replay/demo data is introduced.
