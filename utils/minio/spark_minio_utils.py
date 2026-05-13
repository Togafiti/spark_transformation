import os
from datetime import datetime
from typing import Any, Optional, Sequence
from uuid import uuid4

from pyspark.sql import DataFrame, SparkSession


SUPPORTED_READ_FORMATS = {
    "parquet": {
        "spark_format": "parquet",
        "extensions": (".parquet",),
    },
    "csv": {
        "spark_format": "csv",
        "extensions": (".csv", ".csv.gz"),
    },
    "json": {
        "spark_format": "json",
        "extensions": (".json", ".jsonl", ".ndjson"),
    },
    "text": {
        "spark_format": "text",
        "extensions": (".txt", ".text"),
    },
    "orc": {
        "spark_format": "orc",
        "extensions": (".orc",),
    },
    "avro": {
        "spark_format": "avro",
        "extensions": (".avro",),
    },
    "excel": {
        "spark_format": "com.crealytics.spark.excel",
        "extensions": (".xlsx", ".xls"),
    },
    "xlsx": {
        "spark_format": "com.crealytics.spark.excel",
        "extensions": (".xlsx",),
    },
    "xls": {
        "spark_format": "com.crealytics.spark.excel",
        "extensions": (".xls",),
    },
}


def create_spark_session(
    app_name: str = "MinIO",
    packages: Optional[Sequence[str]] = None,
    existing_spark: Optional[SparkSession] = None,
) -> SparkSession:
    if packages is None:
        packages_raw = (os.getenv("SPARK_PACKAGES") or "").strip()
        if packages_raw:
            packages = tuple(
                package.strip()
                for package in packages_raw.split(",")
                if package.strip()
            )
        else:
            packages = ("org.apache.hadoop:hadoop-aws:3.3.4",)

    packages_csv = ",".join(packages)
    os.environ["PYSPARK_SUBMIT_ARGS"] = f'--packages "{packages_csv}" pyspark-shell'

    if existing_spark is not None:
        try:
            existing_spark.stop()
        except Exception:
            pass

    spark = (
        SparkSession.builder.appName(app_name)
        .config("spark.jars.packages", packages_csv)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def assert_s3a_class_loaded(spark: SparkSession) -> None:
    spark._jvm.Thread.currentThread().getContextClassLoader().loadClass(
        "org.apache.hadoop.fs.s3a.S3AFileSystem"
    )


def configure_minio(
    spark: SparkSession,
    endpoint: str,
    access_key: str,
    secret_key: str,
):
    conf = spark._jsc.hadoopConfiguration()
    conf.set("fs.s3a.endpoint", endpoint)
    conf.set("fs.s3a.access.key", access_key)
    conf.set("fs.s3a.secret.key", secret_key)
    conf.set("fs.s3a.path.style.access", "true")
    conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    conf.set("fs.s3a.connection.ssl.enabled", "false")
    conf.set(
        "fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
    )
    return conf


def check_minio_ready(spark: SparkSession, hadoop_conf, bucket: str) -> int:
    jvm = spark._jvm
    bucket_path = jvm.org.apache.hadoop.fs.Path(f"s3a://{bucket}/")
    file_system = bucket_path.getFileSystem(hadoop_conf)
    if not file_system.exists(bucket_path):
        raise FileNotFoundError(f"Bucket MinIO khong ton tai: {bucket}")
    statuses = file_system.listStatus(bucket_path)
    return len(statuses)


def _normalize_extensions(extensions: Optional[Sequence[str]]) -> Optional[tuple[str, ...]]:
    if not extensions:
        return None
    normalized_extensions = []
    for extension in extensions:
        value = str(extension).strip().lower()
        if not value:
            continue
        if not value.startswith("."):
            value = f".{value}"
        normalized_extensions.append(value)
    return tuple(normalized_extensions) or None


def normalize_read_format(file_format: str) -> str:
    normalized_format = (file_format or "parquet").strip().lower()
    if normalized_format not in SUPPORTED_READ_FORMATS:
        supported = ", ".join(sorted(SUPPORTED_READ_FORMATS))
        raise ValueError(
            f"Unsupported input format '{file_format}'. Supported formats: {supported}"
        )
    return normalized_format


def get_spark_reader_format(file_format: str) -> str:
    normalized_format = normalize_read_format(file_format)
    return SUPPORTED_READ_FORMATS[normalized_format]["spark_format"]


def get_file_extensions_for_format(file_format: str) -> tuple[str, ...]:
    normalized_format = normalize_read_format(file_format)
    return SUPPORTED_READ_FORMATS[normalized_format]["extensions"]


def list_minio_files(
    spark: SparkSession,
    hadoop_conf,
    input_path: str,
    extensions: Optional[Sequence[str]] = None,
) -> list[str]:
    jvm = spark._jvm
    base_path = jvm.org.apache.hadoop.fs.Path(input_path)
    fs = base_path.getFileSystem(hadoop_conf)
    if not fs.exists(base_path):
        raise FileNotFoundError(f"Khong tim thay duong dan input: {input_path}")

    normalized_extensions = _normalize_extensions(extensions)
    file_status = fs.getFileStatus(base_path)
    if file_status.isFile():
        file_path = file_status.getPath().toString()
        lowered_path = file_path.lower()
        if not normalized_extensions or lowered_path.endswith(normalized_extensions):
            return [file_path]
        return []

    files = []
    iterator = fs.listFiles(base_path, True)
    while iterator.hasNext():
        child_status = iterator.next()
        file_path = child_status.getPath().toString()
        lowered_path = file_path.lower()
        if not normalized_extensions or lowered_path.endswith(normalized_extensions):
            files.append(file_path)
    return files


def list_parquet_files(spark: SparkSession, hadoop_conf, input_path: str) -> list[str]:
    return list_minio_files(
        spark,
        hadoop_conf,
        input_path,
        extensions=get_file_extensions_for_format("parquet"),
    )


def validate_minio_input_path(
    spark: SparkSession,
    hadoop_conf,
    input_path: str,
    extensions: Optional[Sequence[str]] = None,
    file_description: str = "parquet",
) -> list[str]:
    files = list_minio_files(spark, hadoop_conf, input_path, extensions=extensions)
    if not files:
        raise FileNotFoundError(
            f"Khong tim thay file {file_description} nao trong: {input_path}"
        )
    return files


def build_unique_output_path(output_base_path: str, run_id: Optional[str] = None) -> str:
    normalized_base_path = output_base_path.rstrip("/")
    if not run_id or not run_id.strip():
        run_id = datetime.now().strftime("%Y%m%dT%H%M%SZ") + "-" + uuid4().hex[:8]
    return f"{normalized_base_path}/run_id={run_id}"


def _normalize_reader_options(options: Optional[dict[str, Any]]) -> dict[str, str]:
    if not options:
        return {}
    normalized_options = {}
    for key, value in options.items():
        if isinstance(value, bool):
            normalized_options[key] = str(value).lower()
        else:
            normalized_options[key] = str(value)
    return normalized_options


def read_minio_data(
    spark: SparkSession,
    hadoop_conf,
    input_path: str,
    file_format: str = "parquet",
    options: Optional[dict[str, Any]] = None,
    extensions: Optional[Sequence[str]] = None,
) -> DataFrame:
    normalized_format = normalize_read_format(file_format)
    spark_format = get_spark_reader_format(normalized_format)
    file_extensions = extensions or get_file_extensions_for_format(normalized_format)

    validate_minio_input_path(
        spark,
        hadoop_conf,
        input_path,
        extensions=file_extensions,
        file_description=normalized_format,
    )

    reader = spark.read.format(spark_format)
    reader_options = _normalize_reader_options(options)
    if reader_options:
        reader = reader.options(**reader_options)
    return reader.load(input_path)


def read_minio_parquet(spark: SparkSession, hadoop_conf, input_path: str) -> DataFrame:
    return read_minio_data(
        spark,
        hadoop_conf,
        input_path,
        file_format="parquet",
    )


def write_minio_parquet(
    df: DataFrame,
    output_base_path: str,
    mode: str = "overwrite",
    unique_run: bool = True,
    run_id: Optional[str] = None,
    partition_cols: Optional[Sequence[str]] = None,
) -> str:
    target_path = (
        build_unique_output_path(output_base_path, run_id=run_id)
        if unique_run
        else output_base_path.rstrip("/")
    )
    writer = df.write.mode(mode)
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    writer.parquet(target_path)
    return target_path
