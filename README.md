# Spark Transformation

Project này chạy pipeline PySpark đọc file từ MinIO, biến đổi dữ liệu, và ghi parquet về MinIO thông qua Airflow.

## Cấu trúc

- `jobs/minio_spark_pipeline.py`: Spark ETL job chính.
- `dags/minio_spark_pipeline_dag.py`: Airflow DAG gọi Spark ETL job.
- `utils/minio/spark_minio_utils.py`: Tiện ích Spark/MinIO.
- `utils/common/`: Các hàm transform DataFrame có thể dùng lại.
- `utils/pipeline/`: Config loader, transform registry, va runner dùng chung cho pipeline.
- `config/pipeline.example.json`: Vi dụ cấu hình extraction, transform, loading.
- `config/pipelines/*.json`: Mỗi file JSON tạo ra một DAG riêng trong Airflow.

## Chạy bằng Docker

Khởi độnng stack:

```bash
docker compose up -d --build
```

Service:

- Airflow UI: http://localhost:8080 (`admin` / `admin`)
- MinIO API: http://localhost:9000
- MinIO Console: http://localhost:9001 (`minio` / `minio123`)

Mặc định Docker compose cấu hình Airflow quét folder:

```text
config/pipelines
```

Mỗi file `*.json` trong folder này sẽ sinh một DAG có dạng:

```text
spark_pipeline_<pipeline_name>
```

Ví dụ `config/pipelines/orders_csv.json` sẽ tạo DAG:

```text
spark_pipeline_orders_csv
```

## Chạy Spark job trực tiếp

```bash
spark-submit jobs/minio_spark_pipeline.py
```

Nếu muốn chạy trực tiếp một config bằng `spark-submit`:

```bash
set PIPELINE_CONFIG_PATH=config/pipelines/orders_csv.json
spark-submit jobs/minio_spark_pipeline.py
```

Trên PowerShell:

```powershell
$env:PIPELINE_CONFIG_PATH = "config/pipelines/orders_csv.json"
spark-submit jobs/minio_spark_pipeline.py
```

## Biến môi trường chính

Bắt buộc:

- `MINIO_ENDPOINT`
- `MINIO_ACCESS_KEY`
- `MINIO_SECRET_KEY`
- `MINIO_BUCKET`
- `MINIO_INPUT_PREFIX` nếu config không có `extraction.input_prefix`
- `MINIO_OUTPUT_PREFIX` nếu config không có `loading.output_prefix`
- `AIRFLOW__WEBSERVER__SECRET_KEY`

Tùy chọn:

- `PIPELINE_RUN_ID`: Run ID cố định; nếu bỏ trống sẽ dùng run ID của Airflow hoặc tự động generate.
- `MINIO_INPUT_FORMAT`: Định dạng input khi config không khai báo `extraction.format`. Mặc định `parquet`.
- `MINIO_INPUT_FILE_EXTENSIONS`: Override extension cần tìm, vi du `.csv,.csv.gz`.
- `PIPELINE_CONFIG_DIR`: Folder Airflow dùng để sinh dynamic DAG. Mặc định `config/pipelines`.
- `PIPELINE_CONFIG_PATH`: Đường dẫn file JSON cấu hình pipeline.
- `PIPELINE_CONFIG_JSON`: Cấu hình pipeline truyền trực tiếp bằng JSON string.
- `MINIO_OUTPUT_PARTITION_COLS`: Danh sách cột partition, cách nhau bằng dấu phẩy. Mặc định `etl_run_date`.
- `SPARK_PROJECT_ROOT`: Project root trong container Airflow.
- `SPARK_JOB_SCRIPT_PATH`: Path Spark ETL script.
- `SPARK_SEED_SCRIPT_PATH`: Path Spark seed script.
- `SPARK_SUBMIT_BIN`: Mặc định `spark-submit`.
- `SPARK_MASTER`: Ví dụ `local[*]`.
- `SPARK_PACKAGES`: Hadoop AWS packages cho `s3a://`.
- `SPARK_EXTRA_CONF`: JSON object để truyền thêm `--conf`.

## Pipeline Config

Mỗi config có thể khai báo metadata trong `pipeline`. Airflow dùng metadata này để tạo DAG:

```json
{
  "pipeline": {
    "name": "orders_csv",
    "schedule": "@daily",
    "enabled": true
  }
}
```

Config orders CSV đang được tích hợp sẵn:

```json
{
  "pipeline": {
    "name": "orders_csv",
    "schedule": "@daily",
    "enabled": true
  },
  "extraction": {
    "input_prefix": "ecommerce_dataset/orders.csv",
    "format": "csv",
    "options": {
      "header": true,
      "inferSchema": true,
      "multiLine": false,
      "escape": "\"",
      "quote": "\""
    }
  },
  "transformation": {
    "steps": [
      {"name": "standardize_column_names"},
      {"name": "drop_duplicates"}
    ]
  },
  "loading": {
    "output_prefix": "transformed/orders",
    "partition_cols": ["etl_run_date"]
  }
}
```

Input của config này:

```text
s3a://warehouse/ecommerce_dataset/orders.csv
```

Output của config này:

```text
s3a://warehouse/transformed/orders/run_id=<dag_run_id>/etl_run_date=<date>
```

Vd đọc parquet:

```json
{
  "extraction": {
    "input_prefix": "products_parquet",
    "format": "parquet",
    "options": {}
  },
  "transformation": {
    "steps": [
      {"name": "standardize_column_names"},
      {"name": "trim", "columns": ["product_name", "category"]},
      {"name": "cast_columns", "cast_map": {"price": "double"}},
      {"name": "drop_duplicates", "subset": ["id"]}
    ]
  },
  "loading": {
    "output_prefix": "products_parquet_transformed",
    "partition_cols": ["etl_run_date"]
  }
}
```

Vd đọc CSV:

```json
{
  "extraction": {
    "input_prefix": "products_csv",
    "format": "csv",
    "options": {
      "header": true,
      "inferSchema": true,
      "multiLine": false
    }
  },
  "transformation": {
    "steps": [
      {"name": "standardize_column_names"}
    ]
  },
  "loading": {
    "output_prefix": "products_csv_transformed",
    "partition_cols": ["etl_run_date"]
  }
}
```

Vd đọc Excel:

```json
{
  "extraction": {
    "input_prefix": "products_excel",
    "format": "excel",
    "options": {
      "header": true,
      "inferSchema": true,
      "dataAddress": "'Sheet1'!A1"
    }
  },
  "transformation": {
    "steps": [
      {"name": "standardize_column_names"}
    ]
  },
  "loading": {
    "output_prefix": "products_excel_transformed",
    "partition_cols": ["etl_run_date"]
  }
}
```

Format input hỗ trợ:

- `parquet`
- `csv`
- `json`
- `text`
- `orc`
- `avro`
- `excel`, `xlsx`, `xls`

Lưu ý: Excel cần Spark data source package `com.crealytics.spark.excel` trong `SPARK_PACKAGES`. CSV, JSON, Parquet, Text va ORC thuong dung duoc voi Spark builtin.

Transform step hiện hỗ trợ:

- `standardize_column_names`
- `rename_columns`
- `cast_columns`
- `select`
- `drop_columns`
- `trim`
- `lowercase`
- `uppercase`
- `fill_null`
- `drop_null`
- `drop_duplicates`
- `filter_rows`
- `sort_rows`

## Lịch Airflow

- Mỗi file trong `config/pipelines/*.json` tạo 1 DAG `spark_pipeline_<name>`.
- `schedule` lấy từ `pipeline.schedule`, mặc định `@daily`.
- Đặt `pipeline.enabled=false` để Airflow bỏ qua config đó.
- `catchup=False`.
- `retries=1`.
