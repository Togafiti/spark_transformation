import os
from typing import Any, Optional

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
    mode: str = "overwrite",
    unique_run: bool = True,
) -> str:
    return write_minio_parquet(
        df,
        output_base_path,
        mode=mode,
        unique_run=unique_run,
        run_id=run_id,
        partition_cols=partition_cols,
    )


def _get_reader_options(input_config: dict[str, Any], section_name: str) -> dict[str, Any]:
    reader_options = input_config.get("options") or {}
    if not isinstance(reader_options, dict):
        raise ValueError(f"Pipeline config '{section_name}.options' must be an object")
    return reader_options


def _extract_input(
    spark,
    hadoop_conf,
    bucket: str,
    input_config: dict[str, Any],
    section_name: str,
) -> tuple[DataFrame, str, str]:
    input_prefix = get_config_or_required_env(
        input_config.get("input_prefix"),
        "MINIO_INPUT_PREFIX",
    )
    input_format = str(
        input_config.get("format")
        or os.getenv("MINIO_INPUT_FORMAT")
        or "parquet"
    ).strip()
    reader_options = _get_reader_options(input_config, section_name)
    file_extensions = get_string_list(
        input_config.get("file_extensions")
        or os.getenv("MINIO_INPUT_FILE_EXTENSIONS"),
        f"{section_name}.file_extensions",
    )
    frame, input_path = extract_stage(
        spark,
        hadoop_conf,
        bucket,
        input_prefix,
        input_format,
        reader_options,
        file_extensions,
    )
    return frame, input_path, input_format


def _extract_inputs(
    spark,
    hadoop_conf,
    bucket: str,
    extraction_config: dict[str, Any],
) -> tuple[DataFrame | None, dict[str, DataFrame], list[str]]:
    input_configs = extraction_config.get("inputs")
    if input_configs is None:
        frame, input_path, _input_format = _extract_input(
            spark,
            hadoop_conf,
            bucket,
            extraction_config,
            "extraction",
        )
        return frame, {}, [input_path]

    if not isinstance(input_configs, list) or not input_configs:
        raise ValueError("Pipeline config 'extraction.inputs' must be a non-empty list")

    frames_by_alias: dict[str, DataFrame] = {}
    input_paths: list[str] = []
    for index, input_config in enumerate(input_configs):
        if not isinstance(input_config, dict):
            raise ValueError(f"Input config at index {index} must be an object")
        alias = str(input_config.get("alias") or "").strip()
        if not alias:
            raise ValueError(f"Input config at index {index} is missing 'alias'")
        if alias in frames_by_alias:
            raise ValueError(f"Duplicate input alias: {alias}")

        frame, input_path, _input_format = _extract_input(
            spark,
            hadoop_conf,
            bucket,
            input_config,
            f"extraction.inputs[{index}]",
        )
        frames_by_alias[alias] = frame
        input_paths.append(input_path)

    return None, frames_by_alias, input_paths


def _apply_sql_transformation(
    spark,
    frames_by_alias: dict[str, DataFrame],
    sql_query: str,
) -> DataFrame:
    if not sql_query.strip():
        raise ValueError("Pipeline config 'transformation.sql' cannot be empty")
    if not frames_by_alias:
        raise ValueError("'transformation.sql' requires 'extraction.inputs'")

    for alias, frame in frames_by_alias.items():
        frame.createOrReplaceTempView(alias)
    return spark.sql(sql_query)


def _normalize_sql_query(sql_query: Any) -> str | None:
    if sql_query is None:
        return None
    if isinstance(sql_query, str):
        return sql_query
    if isinstance(sql_query, list) and all(isinstance(line, str) for line in sql_query):
        return "\n".join(sql_query)
    raise ValueError(
        "Pipeline config 'transformation.sql' must be a string or list of strings"
    )


def run_pipeline() -> None:
    load_env_file()
    pipeline_config = load_pipeline_config()

    for var in REQUIRED_MINIO_ENV_VARS:
        get_required_env(var)

    extraction_config = get_config_section(pipeline_config, "extraction")
    loading_config = get_config_section(pipeline_config, "loading")
    transformation_config = get_config_section(pipeline_config, "transformation")
    transform_steps = get_transform_steps(pipeline_config)

    minio_endpoint = get_required_env("MINIO_ENDPOINT")
    minio_access_key = get_required_env("MINIO_ACCESS_KEY")
    minio_secret_key = get_required_env("MINIO_SECRET_KEY")
    minio_bucket = get_required_env("MINIO_BUCKET")
    minio_output_prefix = get_config_or_required_env(
        loading_config.get("output_prefix"),
        "MINIO_OUTPUT_PREFIX",
    )
    partition_cols_value = (
        loading_config["partition_cols"]
        if "partition_cols" in loading_config
        else os.getenv("MINIO_OUTPUT_PARTITION_COLS") or ["etl_run_date"]
    )
    partition_cols = get_string_list(partition_cols_value, "loading.partition_cols")
    write_mode = str(loading_config.get("mode") or "overwrite").strip()
    unique_run = bool(loading_config.get("unique_run", True))
    sql_query = _normalize_sql_query(transformation_config.get("sql"))

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
        extracted_df, frames_by_alias, input_paths = _extract_inputs(
            spark,
            hadoop_conf,
            minio_bucket,
            extraction_config,
        )

        print(f"MinIO bucket: {minio_bucket}")
        print(f"Bucket visible items: {bucket_item_count}")
        print(f"Input paths: {input_paths}")
        if extracted_df is not None:
            print(f"Total records before transform: {extracted_df.count()}")
        else:
            input_counts = {
                alias: frame.count()
                for alias, frame in frames_by_alias.items()
            }
            print(f"Total records before transform by alias: {input_counts}")
        print(f"Transform steps: {[step['name'] for step in transform_steps]}")
        print(f"SQL transform: {bool(sql_query)}")
        print(f"Output partitions: {partition_cols}")
        print(f"Output unique run path: {unique_run}")

        if sql_query:
            base_df = _apply_sql_transformation(spark, frames_by_alias, sql_query)
        elif extracted_df is not None:
            base_df = extracted_df
        else:
            raise ValueError(
                "Multi-input extraction requires 'transformation.sql'"
            )

        df_transformed = apply_transformation_steps(base_df, transform_steps, pipeline_run_id)

        final_output_path = load_stage(
            df_transformed,
            output_base_path,
            pipeline_run_id,
            partition_cols,
            mode=write_mode,
            unique_run=unique_run,
        )

        print(f"Wrote parquet to: {final_output_path}")

    finally:
        spark.stop()
