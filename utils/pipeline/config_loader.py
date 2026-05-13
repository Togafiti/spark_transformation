import json
import os
import re
from pathlib import Path
from typing import Any, Optional

try:
    from dotenv import load_dotenv
except ImportError:
    def load_dotenv(*args, **kwargs) -> bool:
        return False


REQUIRED_MINIO_ENV_VARS = (
    "MINIO_ENDPOINT",
    "MINIO_ACCESS_KEY",
    "MINIO_SECRET_KEY",
    "MINIO_BUCKET",
)


def load_env_file() -> None:
    dotenv_path = os.getenv("DOTENV_PATH", ".env")
    load_dotenv(dotenv_path=Path(dotenv_path), override=False)


def get_required_env(name: str) -> str:
    value = (os.getenv(name) or "").strip()
    if not value:
        raise ValueError(f"Missing required environment variable: {name}")
    return value


def get_config_or_required_env(config_value: Any, env_name: str) -> str:
    value = str(config_value or os.getenv(env_name, "")).strip()
    if not value:
        raise ValueError(
            f"Missing required setting: provide '{env_name}' or pipeline config value"
        )
    return value


def safe_run_id(run_id: Optional[str]) -> Optional[str]:
    if not run_id:
        return None
    sanitized = re.sub(r"[^a-zA-Z0-9._=-]", "_", run_id.strip())
    return sanitized or None


def load_pipeline_config() -> dict[str, Any]:
    config_json = (os.getenv("PIPELINE_CONFIG_JSON") or "").strip()
    if config_json:
        config = json.loads(config_json)
        if not isinstance(config, dict):
            raise ValueError("PIPELINE_CONFIG_JSON must be a JSON object")
        return config

    config_path = (os.getenv("PIPELINE_CONFIG_PATH") or "").strip()
    if not config_path:
        return {}

    path = Path(config_path)
    if not path.is_absolute():
        path = Path.cwd() / path

    config = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(config, dict):
        raise ValueError(f"Pipeline config must be a JSON object: {path}")
    return config


def get_config_section(config: dict[str, Any], section: str) -> dict[str, Any]:
    value = config.get(section, {})
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise ValueError(f"Pipeline config section '{section}' must be an object")
    return value


def get_transform_steps(config: dict[str, Any]) -> list[dict[str, Any]]:
    transformation = get_config_section(config, "transformation")
    steps = transformation.get("steps", [])
    if steps is None:
        return []
    if not isinstance(steps, list):
        raise ValueError("Pipeline config 'transformation.steps' must be a list")

    for index, step in enumerate(steps):
        if not isinstance(step, dict):
            raise ValueError(f"Transform step at index {index} must be an object")
        if not step.get("name"):
            raise ValueError(f"Transform step at index {index} is missing 'name'")
    return steps


def get_string_list(value: Any, name: str) -> list[str]:
    if value is None or value == "":
        return []
    if isinstance(value, str):
        return [item.strip() for item in value.split(",") if item.strip()]
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    raise ValueError(f"{name} must be a list or comma-separated string")
