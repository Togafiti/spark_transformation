from __future__ import annotations

from collections.abc import Sequence
from typing import Any, Union

from pyspark.sql import DataFrame  # type: ignore[reportMissingImports]
from pyspark.sql import functions as F  # type: ignore[reportMissingImports]
from pyspark.sql.window import Window  # type: ignore[reportMissingImports]


def _build_window(
    partition_by: Sequence[str] | None,
    order_by: Sequence[Union[str, tuple[str, bool]]],
):
    """Tạo window Spark dựa trên các cột phân vùng và sắp xếp đã chỉ định.
    
    order_by có thể là:
    - chuỗi tên cột: ["col"] (mặc định asc)
    - tuple (col_name, asc_bool): [("col", False)] để chỉ định desc
    
    Thứ tự xây dựng window: partition → order.
    """
    if not order_by:
        raise ValueError("Cần khai báo order_by cho lead/lag")

    # Xây dựng danh sách order expressions
    order_exprs = []
    for item in order_by:
        if isinstance(item, str):
            # Mặc định asc
            order_exprs.append(F.col(item).asc())
        elif isinstance(item, tuple) and len(item) == 2:
            col_name, asc = item
            if asc:
                order_exprs.append(F.col(col_name).asc())
            else:
                order_exprs.append(F.col(col_name).desc())
        else:
            raise ValueError(f"order_by phải là str hoặc tuple[str, bool], nhận: {item}")

    if partition_by:
        window_spec = Window.partitionBy(*partition_by).orderBy(*order_exprs)
    else:
        window_spec = Window.orderBy(*order_exprs)
    
    return window_spec


def lag(
    df: DataFrame,
    source_column: str,
    order_by: Sequence[Union[str, tuple[str, bool]]],
    partition_by: Sequence[str] | None = None,
    offset: int = 1,
    default_value: Any | None = None,
    target_column: str | None = None,
) -> DataFrame:
    """Thêm cột lag theo thứ tự đã chỉ định.

    Ví dụ:
        df = lag(df, "amount", order_by=["event_time"], partition_by=["user_id"])
        df = lag(df, "amount", order_by=[("event_time", False)], partition_by=["user_id"])
    """
    target_column = target_column or f"{source_column}_lag_{offset}"
    window_spec = _build_window(partition_by, order_by)
    return df.withColumn(
        target_column,
        F.lag(F.col(source_column), offset, default_value).over(window_spec),
    )


def lead(
    df: DataFrame,
    source_column: str,
    order_by: Sequence[Union[str, tuple[str, bool]]],
    partition_by: Sequence[str] | None = None,
    offset: int = 1,
    default_value: Any | None = None,
    target_column: str | None = None,
) -> DataFrame:
    """Thêm cột lead theo thứ tự đã chỉ định.

    Ví dụ:
        df = lead(df, "amount", order_by=["event_time"], partition_by=["user_id"])
        df = lead(df, "amount", order_by=[("event_time", False)], partition_by=["user_id"])
    """
    target_column = target_column or f"{source_column}_lead_{offset}"
    window_spec = _build_window(partition_by, order_by)
    return df.withColumn(
        target_column,
        F.lead(F.col(source_column), offset, default_value).over(window_spec),
    )


def row_number(
    df: DataFrame,
    order_by: Sequence[Union[str, tuple[str, bool]]],
    partition_by: Sequence[str] | None = None,
    target_column: str = "row_number",
) -> DataFrame:
    """Thêm cột row_number theo thứ tự đã chỉ định.

    Ví dụ:
        df = row_number(df, order_by=["event_time"], partition_by=["user_id"])
        df = row_number(df, order_by=[("amount", False)], partition_by=["user_id"])
    """
    window_spec = _build_window(partition_by, order_by)
    return df.withColumn(target_column, F.row_number().over(window_spec))


def rank(
    df: DataFrame,
    order_by: Sequence[Union[str, tuple[str, bool]]],
    partition_by: Sequence[str] | None = None,
    target_column: str = "rank",
) -> DataFrame:
    """Thêm cột rank theo thứ tự đã chỉ định.

    Ví dụ:
        df = rank(df, order_by=["amount"], partition_by=["user_id"])
        df = rank(df, order_by=[("amount", False)], partition_by=["user_id"])
    """
    window_spec = _build_window(partition_by, order_by)
    return df.withColumn(target_column, F.rank().over(window_spec))


def dense_rank(
    df: DataFrame,
    order_by: Sequence[Union[str, tuple[str, bool]]],
    partition_by: Sequence[str] | None = None,
    target_column: str = "dense_rank",
) -> DataFrame:
    """Thêm cột dense_rank theo thứ tự đã chỉ định.

    Ví dụ:
        df = dense_rank(df, order_by=["amount"], partition_by=["user_id"])
        df = dense_rank(df, order_by=[("amount", False)], partition_by=["user_id"])
    """
    window_spec = _build_window(partition_by, order_by)
    return df.withColumn(target_column, F.dense_rank().over(window_spec))


def ntile(
    df: DataFrame,
    order_by: Sequence[Union[str, tuple[str, bool]]],
    partition_by: Sequence[str] | None = None,
    num_buckets: int = 4,
    target_column: str = "ntile",
) -> DataFrame:
    """Thêm cột ntile theo thứ tự đã chỉ định.

    Ví dụ:
        df = ntile(df, order_by=["amount"], partition_by=["user_id"], num_buckets=4)
        df = ntile(df, order_by=[("amount", False)], partition_by=["user_id"], num_buckets=4)
    """
    window_spec = _build_window(partition_by, order_by)
    return df.withColumn(target_column, F.ntile(num_buckets).over(window_spec))
