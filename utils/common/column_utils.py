from __future__ import annotations

from collections.abc import Mapping, Sequence

from pyspark.sql import DataFrame  # type: ignore[reportMissingImports]
from pyspark.sql import functions as F  # type: ignore[reportMissingImports]


def col(df: DataFrame, column_name: str):
    """Trả về một cột dưới dạng Column expression.

    Ví dụ:
        col(df, "age") + 1
    """
    return F.col(column_name)


def select(df: DataFrame, columns: Sequence[str]) -> DataFrame:
    """Chọn một tập hợp các cột từ DataFrame.

    Ví dụ:
        df = select(df, ["id", "name", "age"])
    """
    return df.select(*columns)


def drop_columns(df: DataFrame, columns: Sequence[str]) -> DataFrame:
    """Loại bỏ một tập hợp các cột khỏi DataFrame.

    Ví dụ:
        df = drop_columns(df, ["temp_col1", "temp_col2"])
    """
    return df.drop(*columns)


def add_column(df: DataFrame, column_name: str, expression) -> DataFrame:
    """Thêm một cột mới vào DataFrame dựa trên một biểu thức.

    Ví dụ:
        df = add_column(df, "age_plus_one", F.col("age") + 1)
    """
    return df.withColumn(column_name, expression)


def rename_column(df: DataFrame, old_name: str, new_name: str) -> DataFrame:
    """Đổi tên một cột.

    Ví dụ:
        df = rename_column(df, "old_name", "new_name")
    """
    return df.withColumnRenamed(old_name, new_name)


def rename_columns(
    df: DataFrame,
    rename_map: Mapping[str, str],
    strict: bool = False,
) -> DataFrame:
    """Đổi tên nhiều cột trong một lần xử lý.

    Ví dụ:
        df = rename_columns(df, {"old_a": "new_a", "old_b": "new_b"})
    """
    current_columns = set(df.columns)
    missing_columns = [source for source in rename_map if source not in current_columns]

    if strict and missing_columns:
        raise KeyError(f"Thiếu các cột: {missing_columns}")

    for source_column, target_column in rename_map.items():
        if source_column in current_columns:
            df = df.withColumnRenamed(source_column, target_column)

    return df


def standardize_column_names(df: DataFrame) -> DataFrame:
    """Chuẩn hóa tên cột bằng cách chuyển sang chữ thường và thay thế khoảng trắng bằng dấu gạch dưới.

    Ví dụ:
        # "Customer Name" → "customer_name"
        df = standardize_column_names(df)
    """
    for column in df.columns:
        standardized_name = column.strip().lower().replace(" ", "_")
        df = df.withColumnRenamed(column, standardized_name)
    return df


def convert_column(
    df: DataFrame,
    column: str,
    target_type: str,
    format: str | None = None,
) -> DataFrame:
    """Chuyển đổi kiểu dữ liệu của một cột bằng một hàm chung.

    Ví dụ:
        df = convert_column(df, "age_str", "integer")
        df = convert_column(df, "date_str", "date", format="yyyy-MM-dd")
    """
    if target_type in {"datetime", "timestamp"}:
        if format:
            return df.withColumn(column, F.to_timestamp(F.col(column), format))
        return df.withColumn(column, F.to_timestamp(F.col(column)))

    if target_type == "date":
        if format:
            return df.withColumn(column, F.to_date(F.col(column), format))
        return df.withColumn(column, F.to_date(F.col(column)))

    return df.withColumn(column, F.col(column).cast(target_type))


def cast_column(df: DataFrame, column: str, target_type: str, format: str | None = None) -> DataFrame:
    """Chuyển đổi kiểu dữ liệu của một cột.

    Ví dụ:
        df = cast_column(df, "age", "integer")
        df = cast_column(df, "age", "string")
        df = to_datetime_column(df, "date_str", "timestamp", "yyyy-MM-dd")
    """
    return convert_column(df, column, target_type, format)


def cast_columns(df: DataFrame, cast_map: Mapping[str, str], format_map: Mapping[str, str | None] | None = None) -> DataFrame:
    """Chuyển đổi kiểu dữ liệu của nhiều cột trong một lần xử lý.

    Ví dụ:
        df = cast_columns(df, {"age": "integer", "price": "double"})
        df = cast_columns(df, {"date": "date"}, {"date": "yyyy-MM-dd"})
    """
    for column, target_type in cast_map.items():
        format = format_map.get(column) if format_map else None
        df = convert_column(df, column, target_type, format)
    return df