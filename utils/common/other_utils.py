from __future__ import annotations

from functools import reduce
from operator import and_
from collections.abc import Sequence
from typing import Union

from pyspark.sql import DataFrame  # type: ignore[reportMissingImports]
from pyspark.sql import functions as F  # type: ignore[reportMissingImports]


def filter_rows(df: DataFrame, conditions: Sequence[str]) -> DataFrame:
    """Lọc các dòng trong DataFrame dựa trên một hoặc nhiều điều kiện.

    Ví dụ:
        df_filtered = filter_rows(df, ["age > 30", "country = 'USA'"])
    """
    combined_condition = reduce(and_, (F.expr(condition) for condition in conditions))
    return df.filter(combined_condition)


def sort_rows(
    df: DataFrame,
    columns: Sequence[str],
    ascending: Union[bool, Sequence[bool]] = True,
) -> DataFrame:
    """Sắp xếp các dòng trong DataFrame theo một hoặc nhiều cột.

    Tham số `ascending` có thể là:
    - một `bool` áp dụng cho tất cả cột (mặc định `True`),
    - hoặc một `Sequence[bool]` có cùng độ dài với `columns` để chỉ định
      asc/desc cho từng cột tương ứng.

    Ví dụ:
        # Tất cả giảm dần
        df_sorted = sort_rows(df, ["age", "name"], ascending=False)

        # age giảm dần, name tăng dần
        df_sorted = sort_rows(df, ["age", "name"], ascending=[False, True])
    """
    # Nếu ascending là bool, dùng DataFrame.sort trực tiếp
    if isinstance(ascending, bool):
        return df.sort(*columns, ascending=ascending)

    # Nếu ascending là danh sách boolean, build lại các Column expressions
    if len(columns) != len(ascending):
        raise ValueError("Danh sách `ascending` phải cùng độ dài với `columns`")

    sort_exprs = []
    for col, asc in zip(columns, ascending):
        expr = F.col(col).asc() if asc else F.col(col).desc()
        sort_exprs.append(expr)

    return df.sort(*sort_exprs)


# def print_schema(df: DataFrame) -> None:
#     """In ra schema của DataFrame một cách dễ đọc.

#     Ví dụ:
#         print_schema(df)
#     """
#     print("Schema của DataFrame:")
#     for field in df.schema.fields:
#         print(f" - {field.name}: {field.dataType}")


def type_of(df: DataFrame, column: str) -> str:
    """Trả về kiểu dữ liệu của một cột trong DataFrame.

    Ví dụ:
        col_type = type_of(df, "age")
    """
    return dict(df.dtypes)[column]