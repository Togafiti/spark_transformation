from __future__ import annotations

from collections.abc import Mapping, Sequence

from pyspark.sql import DataFrame  # type: ignore[reportMissingImports]
from pyspark.sql import functions as F  # type: ignore[reportMissingImports]


def aggregate_data(
    df: DataFrame,
    group_by: Sequence[str],
    aggregations: Mapping[str, str],
) -> DataFrame:
    """Thực hiện các phép tổng hợp trên một DataFrame dựa trên các cột nhóm và phép tính đã chỉ định.

    Ví dụ:
        df_agg = aggregate_data(df, group_by=["category"], aggregations={"sales": "sum", "quantity": "avg"})
    """
    agg_expressions = [getattr(F, agg_func)(col).alias(f"{col}_{agg_func}") for col, agg_func in aggregations.items()]
    return df.groupBy(*group_by).agg(*agg_expressions)


def pivot_data(
    df: DataFrame,
    group_by: Sequence[str],
    pivot_column: str,
    value_column: str,
    agg_func: str = "sum",
) -> DataFrame:
    """Thực hiện phép pivot trên một DataFrame dựa trên cột pivot và phép tính đã chỉ định.

    Ví dụ:
        df_pivot = pivot_data(df, group_by=["date"], pivot_column="category", value_column="sales", agg_func="sum")
    """
    return df.groupBy(*group_by).pivot(pivot_column).agg(getattr(F, agg_func)(value_column))


def sum(df: DataFrame, sum_columns: str) -> DataFrame:
    """Tính tổng các cột chỉ định

    Ví dụ:
        df_sum = sum(df, sum_columns="quantity")
    """
    return df.agg(F.sum(sum_columns).alias(f"{sum_columns}_sum"))


def avg(df: DataFrame, avg_columns: str) -> DataFrame:
    """Tính trung bình các cột chỉ định

    Ví dụ:
        df_avg = avg(df, avg_columns="quantity")
    """
    return df.agg(F.avg(avg_columns).alias(f"{avg_columns}_avg"))


def min(df: DataFrame, min_column: str) -> DataFrame:
    """Tính giá trị nhỏ nhất của 1 cột.

    Ví dụ:
        df_min = min(df, min_column="price")
    """
    return df.agg(F.min(min_column).alias(f"{min_column}_min"))


def max(df: DataFrame, max_column: str) -> DataFrame:
    """Tính giá trị lớn nhất của 1 cột.

    Ví dụ:
        df_max = max(df, max_column="price")
    """
    return df.agg(F.max(max_column).alias(f"{max_column}_max"))