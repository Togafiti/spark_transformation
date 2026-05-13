from __future__ import annotations

from collections.abc import Sequence

from pyspark.sql import DataFrame  # type: ignore[reportMissingImports]


def drop_duplicates(df: DataFrame, subset: Sequence[str] | None = None) -> DataFrame:
    """Loại bỏ các dòng trùng lặp, có thể giới hạn theo nhiều cột.

    Ví dụ:
        df = drop_duplicates(df, subset=["id", "date"])
    """
    if subset:
        return df.dropDuplicates(list(subset))
    return df.dropDuplicates()