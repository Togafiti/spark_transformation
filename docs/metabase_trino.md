# Metabase + Trino

This project exposes MinIO Parquet data through Trino, then connects Metabase to Trino for dashboards.

## Services

- Trino UI: http://localhost:8082
- Metabase UI: http://localhost:3000
- MinIO Console: http://localhost:9001

## Metabase Database Connection

In Metabase, add a database with these settings:

- Database type: `Starburst (Trino)`
- Display name: `Warehouse Gold`
- Host: `trino`
- Port: `8080`
- Catalog: `hive`
- Schema: `gold`
- Username: `metabase`
- Password: leave empty
- SSL: disabled

Use `trino:8080` inside Docker because Metabase runs in the same Compose network. Use `localhost:8082` only from the host browser or host CLI.

## Gold Tables

The Trino bootstrap service creates external tables for:

- `hive.gold.dim_users`
- `hive.gold.dim_products`
- `hive.gold.fact_orders`
- `hive.gold.fact_order_items`
- `hive.gold.fact_events`
- `hive.gold.daily_sales`
- `hive.gold.product_performance`

After Airflow finishes writing partitioned gold tables, sync Hive partition metadata:

```powershell
docker compose exec trino trino --server http://localhost:8080 --file /sql/sync_gold_partitions.sql
```

Then refresh/sync the database in Metabase Admin settings so new rows and fields are visible.

## Quick Trino Checks

```powershell
docker compose exec trino trino --server http://localhost:8080 --execute "SHOW SCHEMAS FROM hive"
docker compose exec trino trino --server http://localhost:8080 --execute "SHOW TABLES FROM hive.gold"
docker compose exec trino trino --server http://localhost:8080 --execute "SELECT * FROM hive.gold.product_performance LIMIT 10"
```
