import json
import os
import re
import subprocess
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from airflow import DAG
from airflow.operators.python import PythonOperator

from utils.dag.spark_dag_utils import build_spark_command, read_setting


DEFAULT_DAG_PREFIX = "spark_pipeline"
DEFAULT_SCHEDULE = "@daily"


def _safe_dag_suffix(value: str) -> str:
    suffix = re.sub(r"[^a-zA-Z0-9_]+", "_", value.strip().lower())
    suffix = re.sub(r"_+", "_", suffix).strip("_")
    return suffix or "pipeline"


def _load_pipeline_config(config_path: Path) -> dict[str, Any]:
    config = json.loads(config_path.read_text(encoding="utf-8"))
    if not isinstance(config, dict):
        raise ValueError(f"Pipeline config must be a JSON object: {config_path}")
    return config


def _pipeline_metadata(config_path: Path) -> dict[str, Any]:
    config = _load_pipeline_config(config_path)
    metadata = config.get("pipeline") or {}
    if not isinstance(metadata, dict):
        raise ValueError(
            f"Pipeline config section 'pipeline' must be an object: {config_path}"
        )
    return metadata


def _discover_pipeline_configs() -> list[Path]:
    project_root = os.getenv("SPARK_PROJECT_ROOT", os.getcwd()).strip() or os.getcwd()
    default_config_dir = str(Path(project_root) / "config" / "pipelines")
    config_dir = Path(os.getenv("PIPELINE_CONFIG_DIR", default_config_dir).strip())

    if not config_dir.is_absolute():
        config_dir = Path(project_root) / config_dir

    if not config_dir.exists():
        print(f"Pipeline config dir not found: {config_dir}")
        return []

    return sorted(config_dir.glob("*.json"))


def _run_spark_job(config_path: str, **context) -> None:
    env = os.environ.copy()

    required_env_vars = (
        "MINIO_ENDPOINT",
        "MINIO_ACCESS_KEY",
        "MINIO_SECRET_KEY",
        "MINIO_BUCKET",
    )

    for key in required_env_vars:
        env[key] = read_setting(key)

    optional_env_vars = (
        "MINIO_INPUT_PREFIX",
        "MINIO_INPUT_FORMAT",
        "MINIO_INPUT_FILE_EXTENSIONS",
        "MINIO_OUTPUT_PREFIX",
        "MINIO_OUTPUT_PARTITION_COLS",
        "SPARK_PACKAGES",
    )
    for key in optional_env_vars:
        value = read_setting(key)
        if value:
            env[key] = value

    missing_vars = [key for key in required_env_vars if not env.get(key)]
    if missing_vars:
        raise ValueError(
            f"Missing required MinIO settings: {', '.join(missing_vars)}"
        )

    dag_run = context.get("dag_run")
    if dag_run and dag_run.run_id:
        env["PIPELINE_RUN_ID"] = dag_run.run_id

    project_root = read_setting("SPARK_PROJECT_ROOT", os.getcwd())
    env["PYTHONPATH"] = (
        f"{project_root}{os.pathsep}{env.get('PYTHONPATH', '')}".rstrip(os.pathsep)
    )
    env["PIPELINE_CONFIG_PATH"] = config_path

    script_path = read_setting("SPARK_JOB_SCRIPT_PATH", "jobs/minio_spark_pipeline.py")
    command = build_spark_command(script_path)
    print(f"Pipeline config: {config_path}")
    print("Executing command:", " ".join(command))
    subprocess.run(command, env=env, cwd=project_root, check=True)


default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


def _create_pipeline_dag(config_path: Path) -> DAG | None:
    metadata = _pipeline_metadata(config_path)
    if metadata.get("enabled", True) is False:
        return None

    pipeline_name = str(metadata.get("name") or config_path.stem)
    dag_id = f"{DEFAULT_DAG_PREFIX}_{_safe_dag_suffix(pipeline_name)}"
    schedule = metadata.get("schedule", DEFAULT_SCHEDULE)
    tags = ["spark", "minio", "etl", _safe_dag_suffix(pipeline_name)]

    dag = DAG(
        dag_id=dag_id,
        default_args=default_args,
        description=f"Run PySpark MinIO ETL for {pipeline_name}",
        start_date=datetime(2026, 1, 1),
        schedule=schedule,
        catchup=False,
        tags=tags,
    )

    with dag:
        PythonOperator(
            task_id="run_pyspark_minio_etl",
            python_callable=_run_spark_job,
            op_kwargs={"config_path": str(config_path)},
        )

    return dag


for pipeline_config_path in _discover_pipeline_configs():
    pipeline_dag = _create_pipeline_dag(pipeline_config_path)
    if pipeline_dag is not None:
        globals()[pipeline_dag.dag_id] = pipeline_dag
