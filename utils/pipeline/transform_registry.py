from typing import Any, Optional

from pyspark.sql import DataFrame
from pyspark.sql.functions import current_timestamp, lit, to_date

from utils.common.cleaning_utils import drop_null, fill_null
from utils.common.column_utils import (
    cast_columns,
    drop_columns,
    rename_columns,
    select,
    standardize_column_names,
)
from utils.common.dedup_utils import drop_duplicates
from utils.common.other_utils import filter_rows, sort_rows
from utils.common.string_utils import lowercase, trim, uppercase


TRANSFORM_REGISTRY = {
    "standardize_column_names": standardize_column_names,
    "rename_columns": rename_columns,
    "cast_columns": cast_columns,
    "select": select,
    "drop_columns": drop_columns,
    "trim": trim,
    "lowercase": lowercase,
    "uppercase": uppercase,
    "fill_null": fill_null,
    "drop_null": drop_null,
    "drop_duplicates": drop_duplicates,
    "filter_rows": filter_rows,
    "sort_rows": sort_rows,
}


def apply_transformation_steps(
    df: DataFrame,
    steps: list[dict[str, Any]],
    run_id: Optional[str],
) -> DataFrame:
    for step in steps:
        step_name = str(step["name"]).strip()
        transform = TRANSFORM_REGISTRY.get(step_name)
        if transform is None:
            available_steps = ", ".join(sorted(TRANSFORM_REGISTRY))
            raise ValueError(
                f"Unsupported transform step '{step_name}'. "
                f"Available steps: {available_steps}"
            )

        params = {key: value for key, value in step.items() if key != "name"}
        df = transform(df, **params)

    return (
        df.withColumn("etl_loaded_at", current_timestamp())
        .withColumn("etl_run_date", to_date(current_timestamp()))
        .withColumn("etl_run_id", lit(run_id or "manual"))
    )
