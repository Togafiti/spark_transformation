from __future__ import annotations

from functools import reduce
from operator import and_
from collections.abc import Sequence

from pyspark.sql import DataFrame  # type: ignore[reportMissingImports]
from pyspark.sql import functions as F  # type: ignore[reportMissingImports]


def join_dataframes(
    left_df: DataFrame,
    right_df: DataFrame,
    left_keys: Sequence[str],
    right_keys: Sequence[str] | None = None,
    join_type: str = "inner",
) -> DataFrame:
    """Thực hiện phép nối giữa hai DataFrame dựa trên các cột khóa đã chỉ định.

    Ví dụ:
        df_joined = join_dataframes(df1, df2, left_keys=["id"], join_type="left")
        df_joined = join_dataframes(
            df1,
            df2,
            left_keys=["id_left"],
            right_keys=["id_right"],
            join_type="left",
        )
    """
    right_keys = right_keys or left_keys

    if len(left_keys) != len(right_keys):
        raise ValueError("left_keys và right_keys phải có cùng số lượng cột")

    join_conditions = [left_df[left_key] == right_df[right_key] for left_key, right_key in zip(left_keys, right_keys)]
    join_condition = reduce(and_, join_conditions)
    return left_df.join(right_df, on=join_condition, how=join_type)


def broadcast_join_dataframes(
    left_df: DataFrame,
    right_df: DataFrame,
    left_keys: Sequence[str],
    right_keys: Sequence[str] | None = None,
    join_type: str = "inner",
) -> DataFrame:
    """Thực hiện phép nối giữa hai DataFrame với bảng bên phải được broadcast."""

    right_df_broadcasted = F.broadcast(right_df)
    return join_dataframes(left_df, right_df_broadcasted, left_keys, right_keys, join_type)


def cross_join_dataframes(left_df: DataFrame, right_df: DataFrame) -> DataFrame:
    """Thực hiện phép nối chéo giữa hai DataFrame."""

    return left_df.crossJoin(right_df)


def join_with_aliases(
    left_df: DataFrame,
    right_df: DataFrame,
    left_alias: str,
    right_alias: str,
    left_keys: Sequence[str],
    right_keys: Sequence[str] | None = None,
    join_type: str = "inner",
) -> DataFrame:
    """Thực hiện phép nối giữa hai DataFrame với bí danh để tránh xung đột tên cột."""

    right_keys = right_keys or left_keys

    if len(left_keys) != len(right_keys):
        raise ValueError("left_keys và right_keys phải có cùng số lượng cột")

    left_df_aliased = left_df.alias(left_alias)
    right_df_aliased = right_df.alias(right_alias)

    join_conditions = [
        left_df_aliased[left_key] == right_df_aliased[right_key]
        for left_key, right_key in zip(left_keys, right_keys)
    ]
    join_condition = reduce(and_, join_conditions)

    return left_df_aliased.join(right_df_aliased, on=join_condition, how=join_type)


def self_join(df: DataFrame, keys: Sequence[str], join_type: str = "inner") -> DataFrame:
    """Thực hiện phép nối giữa một DataFrame với chính nó."""

    return join_with_aliases(df, df, "left", "right", keys, keys, join_type)