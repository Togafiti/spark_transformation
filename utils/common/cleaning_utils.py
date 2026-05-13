from __future__ import annotations

from typing import Any

from pyspark.sql import DataFrame  # type: ignore[reportMissingImports]
from pyspark.sql import functions as F  # type: ignore[reportMissingImports]


def fill_null(df: DataFrame, column: str, value: Any) -> DataFrame:
    """Điền giá trị cố định cho các ô null trong một cột.

    Ví dụ:
        df = fill_null(df, "country", "unknown")
    """
    return df.na.fill({column: value})


def drop_null(df: DataFrame, column: str) -> DataFrame:
    """Xóa các dòng có giá trị null ở cột được chọn.

    Ví dụ:
        df = drop_null(df, "customer_id")
    """
    return df.na.drop(subset=[column])


def coalesce_column(df: DataFrame, target_column: str, *source_columns: str) -> DataFrame:
    """Tạo một cột mới từ giá trị không null đầu tiên trong các cột nguồn.

    Ví dụ:
        # Tạo "full_name" (cột mới) từ "nickname", "name" (cột nguồn)
        df = coalesce_column(df, "full_name", "nickname", "name")
    """
    if not source_columns:
        raise ValueError("Cần ít nhất một cột nguồn")

    return df.withColumn(target_column, F.coalesce(*[F.col(column) for column in source_columns]))


def ifnull_column(df: DataFrame, target_column: str, column: str, default_value: Any) -> DataFrame:
    """Thay thế giá trị null của một cột nguồn và lưu kết quả vào cột mới.

    Ví dụ:
        # Lấy cột "status", nếu null thì thay bằng "new", kết quả lưu vào "status_filled"
        # ["active", null, "inactive"] → ["active", "new", "inactive"]
        df = ifnull_column(df, "status_filled", "status", "new")
    """
    return df.withColumn(target_column, F.coalesce(F.col(column), F.lit(default_value)))


