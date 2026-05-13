import os
from typing import Optional

from pyspark.sql import DataFrame
from pyspark.sql.functions import lit

from utils.minio.spark_minio_utils import (
    assert_s3a_class_loaded,
    check_minio_ready,
    configure_minio,
    create_spark_session,
    get_file_extensions_for_format,
    read_minio_data,
    validate_minio_input_path,
    write_minio_parquet,
)
from utils.pipeline.config_loader import (
    REQUIRED_MINIO_ENV_VARS,
    get_config_or_required_env,
    get_config_section,
    get_required_env,
    get_string_list,
    get_transform_steps,
    load_env_file,
    load_pipeline_config,
    safe_run_id,
)
from utils.pipeline.transform_registry import apply_transformation_steps


def extract_stage(
    spark,
    hadoop_conf,
    bucket: str,
    input_prefix: str,
    input_format: str,
    reader_options: dict,
    file_extensions: list[str],
) -> tuple[DataFrame, str]:
    input_path = f"s3a://{bucket}/{input_prefix}".rstrip("/")
    extensions = file_extensions or list(get_file_extensions_for_format(input_format))

    input_files = validate_minio_input_path(
        spark,
        hadoop_conf,
        input_path,
        extensions=extensions,
        file_description=input_format,
    )
    frame = read_minio_data(
        spark,
        hadoop_conf,
        input_path,
        file_format=input_format,
        options=reader_options,
        extensions=extensions,
    )
    frame = frame.withColumn("source_path", lit(input_path))

    print(f"Input path: {input_path}")
    print(f"Input format: {input_format}")
    print(f"Input files found: {len(input_files)}")

    return frame, input_path


def load_stage(
    df: DataFrame,
    output_base_path: str,
    run_id: Optional[str],
    partition_cols: list[str],
) -> str:
    return write_minio_parquet(
        df,
        output_base_path,
        mode="overwrite",
        unique_run=True,
        run_id=run_id,
        partition_cols=partition_cols,
    )


def run_pipeline() -> None:
    load_env_file()
    pipeline_config = load_pipeline_config()

    for var in REQUIRED_MINIO_ENV_VARS:
        get_required_env(var)

    extraction_config = get_config_section(pipeline_config, "extraction")
    loading_config = get_config_section(pipeline_config, "loading")
    transform_steps = get_transform_steps(pipeline_config)

    minio_endpoint = get_required_env("MINIO_ENDPOINT")
    minio_access_key = get_required_env("MINIO_ACCESS_KEY")
    minio_secret_key = get_required_env("MINIO_SECRET_KEY")
    minio_bucket = get_required_env("MINIO_BUCKET")
    minio_input_prefix = get_config_or_required_env(
        extraction_config.get("input_prefix"),
        "MINIO_INPUT_PREFIX",
    )
    input_format = str(
        extraction_config.get("format")
        or os.getenv("MINIO_INPUT_FORMAT")
        or "parquet"
    ).strip()
    reader_options = extraction_config.get("options") or {}
    if not isinstance(reader_options, dict):
        raise ValueError("Pipeline config 'extraction.options' must be an object")
    file_extensions = get_string_list(
        extraction_config.get("file_extensions")
        or os.getenv("MINIO_INPUT_FILE_EXTENSIONS"),
        "extraction.file_extensions",
    )
    minio_output_prefix = get_config_or_required_env(
        loading_config.get("output_prefix"),
        "MINIO_OUTPUT_PREFIX",
    )
    partition_cols = get_string_list(
        loading_config.get("partition_cols")
        or os.getenv("MINIO_OUTPUT_PARTITION_COLS")
        or ["etl_run_date"],
        "loading.partition_cols",
    )

    pipeline_run_id = safe_run_id(
        (os.getenv("PIPELINE_RUN_ID") or "").strip() or None
    )

    output_base_path = f"s3a://{minio_bucket}/{minio_output_prefix}".rstrip("/")

    spark = create_spark_session(app_name="PySpark_MinIO_Parquet_Airflow")

    try:
        assert_s3a_class_loaded(spark)

        hadoop_conf = configure_minio(
            spark,
            minio_endpoint,
            minio_access_key,
            minio_secret_key,
        )

        bucket_item_count = check_minio_ready(spark, hadoop_conf, minio_bucket)
        extracted_df, input_path = extract_stage(
            spark,
            hadoop_conf,
            minio_bucket,
            minio_input_prefix,
            input_format,
            reader_options,
            file_extensions,
        )

        print(f"MinIO bucket: {minio_bucket}")
        print(f"Bucket visible items: {bucket_item_count}")
        print(f"Input path: {input_path}")
        print(f"Total records before transform: {extracted_df.count()}")
        print(f"Transform steps: {[step['name'] for step in transform_steps]}")
        print(f"Output partitions: {partition_cols}")

        df_transformed = apply_transformation_steps(
            extracted_df,
            transform_steps,
            pipeline_run_id,
        )

        final_output_path = load_stage(
            df_transformed,
            output_base_path,
            pipeline_run_id,
            partition_cols,
        )

        print(f"Wrote parquet to: {final_output_path}")

    finally:
        spark.stop()
