# Metabase + Trino

Dự án này tích hợp hiển thị dữ liệu MinIO Parquet thông qua Trino, sau đó kết nối Metabase với Trino để tạo dashboard.

- Trino UI: http://localhost:8082
- Metabase UI: http://localhost:3000
- MinIO Console: http://localhost:9001

## Kết nối cơ sở dữ liệu Metabase

Trong Metabase, hãy thêm cơ sở dữ liệu với các thiết lập sau:

- Database type: `Starburst (Trino)`
- Display name: `Warehouse Gold`
- Host: `trino`
- Port: `8080`
- Catalog: `hive`
- Schema: `gold`
- Username: `metabase`
- Password: leave empty
- SSL: disabled

Hãy sử dụng `trino:8080` bên trong Docker vì Metabase chạy trong cùng mạng Compose. Chỉ sử dụng `localhost:8082` từ trình duyệt hoặc giao diện dòng lệnh của máy chủ.

## Gold Tables

Dịch vụ khởi tạo Trino tạo các external tables cho:

- `hive.gold.dim_users`
- `hive.gold.dim_products`
- `hive.gold.fact_orders`
- `hive.gold.fact_order_items`
- `hive.gold.fact_events`
- `hive.gold.daily_sales`
- `hive.gold.product_performance`

Sau khi Airflow hoàn tất việc ghi các bảng vàng được phân vùng, hãy đồng bộ Hive partition metadata:

```powershell
docker compose exec trino trino --server http://localhost:8080 --file /sql/sync_gold_partitions.sql
```

Sau đó refresh/sync the database (làm mới/đồng bộ hóa cơ sở dữ liệu) in Metabase Admin settings (cài đặt quản trị Metabase) để các hàng và trường mới hiển thị.

## Kiểm tra nhanh Trino

```powershell
docker compose exec trino trino --server http://localhost:8080 --execute "SHOW SCHEMAS FROM hive"
docker compose exec trino trino --server http://localhost:8080 --execute "SHOW TABLES FROM hive.gold"
docker compose exec trino trino --server http://localhost:8080 --execute "SELECT * FROM hive.gold.product_performance LIMIT 10"
```
