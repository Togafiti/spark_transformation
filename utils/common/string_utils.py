from __future__ import annotations

from collections.abc import Mapping, Sequence

from pyspark.sql import DataFrame  # type: ignore[reportMissingImports]
from pyspark.sql import functions as F  # type: ignore[reportMissingImports]


def trim(df: DataFrame, columns: Sequence[str]) -> DataFrame:
    """Loại bỏ khoảng trắng ở đầu và cuối của các cột chuỗi.

    Ví dụ:
        df = trim(df, ["name", "address"])
    """
    for column in columns:
        df = df.withColumn(column, F.trim(F.col(column)))
    return df


def lowercase(df: DataFrame, columns: Sequence[str]) -> DataFrame:
    """Chuyển đổi các cột chuỗi thành chữ thường.

    Ví dụ:
        df = lowercase(df, ["email"])
    """
    for column in columns:
        df = df.withColumn(column, F.lower(F.col(column)))
    return df


def uppercase(df: DataFrame, columns: Sequence[str]) -> DataFrame:
    """Chuyển đổi các cột chuỗi thành chữ hoa.

    Ví dụ:
        df = uppercase(df, ["country"])
    """
    for column in columns:
        df = df.withColumn(column, F.upper(F.col(column)))
    return df


def rename_values(df: DataFrame, column: str, rename_map: Mapping[str, str]) -> DataFrame:
    """Đổi tên các giá trị cụ thể trong một cột.

    Ví dụ:
        df = rename_values(df, "status", {"active": "A", "inactive": "I"})
    """
    mapping_expr = F.create_map(
        *[F.lit(item) for pair in rename_map.items() for item in pair]
    )
    return df.withColumn(column, mapping_expr.getItem(F.col(column)).otherwise(F.col(column)))


def replace_substring(df: DataFrame, column: str, pattern: str, replacement: str) -> DataFrame:
    """Thay thế một chuỗi con trong cột bằng một chuỗi khác.

    Ví dụ:
        df = replace_substring(df, "email", r"@example\.com$", "@newdomain.com")
    """
    return df.withColumn(column, F.regexp_replace(F.col(column), pattern, replacement))


def regex_extract_column(df: DataFrame, source_column: str, target_column: str, pattern: str) -> DataFrame:
    """Tạo một cột mới bằng cách trích xuất chuỗi con khớp với biểu thức chính quy.

    Ví dụ:
        df = regex_extract_column(df, "email", "username", r"^([^@]+)@")
    """
    return df.withColumn(target_column, F.regexp_extract(F.col(source_column), pattern, 1))


def substring_column(df: DataFrame, source_column: str, target_column: str, start: int, length: int) -> DataFrame:
    """Tạo một cột mới bằng cách lấy một phần của cột nguồn.

    Ví dụ:
        df = substring_column(df, "full_name", "first_name", 1, 5)
    """
    return df.withColumn(target_column, F.substring(F.col(source_column), start, length))


def split_column(df: DataFrame, source_column: str, target_columns: Sequence[str], separator: str = " ") -> DataFrame:
    """Tạo nhiều cột mới bằng cách tách cột nguồn theo một dấu phân cách.

    Ví dụ:
        df = split_column(df, "full_name", ["first_name", "last_name"], separator=" ")
    """
    split_col = F.split(F.col(source_column), separator)
    for i, target_column in enumerate(target_columns):
        df = df.withColumn(target_column, split_col.getItem(i))
    return df


def concat_columns(df: DataFrame, source_columns: Sequence[str], target_column: str, separator: str = " ") -> DataFrame:
    """Tạo một cột mới bằng cách nối các cột nguồn với một dấu phân cách.

    Ví dụ:
        df = concat_columns(df, ["first_name", "last_name"], "full_name", separator=" ")
    """
    return df.withColumn(target_column, F.concat_ws(separator, *[F.col(col) for col in source_columns]))


# def length_of_column(df: DataFrame, source_column: str, target_column: str) -> DataFrame:
#     """Tạo một cột mới chứa độ dài của cột nguồn.

#     Ví dụ:
#         df = length_of_column(df, "description", "description_length")
#     """
#     return df.withColumn(target_column, F.length(F.col(source_column)))


def contains_substring_column(df: DataFrame, source_column: str, target_column: str, substring: str) -> DataFrame:
    """Tạo một cột mới boolean cho biết cột nguồn có chứa chuỗi con hay không.

    Ví dụ:
        df = contains_substring_column(df, "email", "has_gmail", "@gmail.com")
    """
    return df.withColumn(target_column, F.col(source_column).contains(substring))


def starts_with(df: DataFrame, source_column: str, substring: str) -> DataFrame:
    """Lọc các dòng mà cột nguồn bắt đầu bằng chuỗi con.

    Ví dụ:
        df = starts_with(df, "url", "https://")
    """
    return df.filter(F.col(source_column).startswith(substring))


def ends_with(df: DataFrame, source_column: str, substring: str) -> DataFrame:
    """Lọc các dòng mà cột nguồn kết thúc bằng chuỗi con.

    Ví dụ:
        df = ends_with(df, "filename", ".csv")
    """
    return df.filter(F.col(source_column).endswith(substring))


def starts_with_column(df: DataFrame, source_column: str, target_column: str, substring: str) -> DataFrame:
    """Tạo một cột mới boolean cho biết cột nguồn có bắt đầu bằng chuỗi con hay không.

    Ví dụ:
        df = starts_with_column(df, "url", "is_secure", "https://")
    """
    return df.withColumn(target_column, F.col(source_column).startswith(substring))


def ends_with_column(df: DataFrame, source_column: str, target_column: str, substring: str) -> DataFrame:
    """Tạo một cột mới boolean cho biết cột nguồn có kết thúc bằng chuỗi con hay không.

    Ví dụ:
        df = ends_with_column(df, "filename", "is_csv", ".csv")
    """
    return df.withColumn(target_column, F.col(source_column).endswith(substring))


def is_null_or_empty_column(df: DataFrame, source_column: str, target_column: str) -> DataFrame:
    """Tạo một cột mới boolean cho biết cột nguồn có giá trị null hoặc chuỗi rỗng hay không.

    Ví dụ:
        df = is_null_or_empty_column(df, "nickname", "is_nickname_missing")
    """
    return df.withColumn(target_column, F.col(source_column).isNull() | (F.col(source_column) == ""))


def mask_column(df: DataFrame, source_column: str, target_column: str, mask_char: str = "*", unmasked_length: int = 4) -> DataFrame:
    """Tạo một cột mới bằng cách che giấu phần đầu của cột nguồn, chỉ giữ lại một số ký tự cuối.

    Ví dụ:
        df = mask_column(df, "credit_card_number", "masked_cc", mask_char="*", unmasked_length=4)
    """
    return df.withColumn(
        target_column,
        F.concat(
            F.lit(mask_char * (F.length(F.col(source_column)) - unmasked_length)),
            F.substring(F.col(source_column), -unmasked_length, unmasked_length),
        )
    )


def remove_special_characters_column(df: DataFrame, source_column: str, target_column: str) -> DataFrame:
    """Tạo một cột mới bằng cách loại bỏ tất cả các ký tự đặc biệt khỏi cột nguồn.

    Ví dụ:
        df = remove_special_characters_column(df, "phone_number", "clean_phone_number")
    """
    return df.withColumn(target_column, F.regexp_replace(F.col(source_column), r"[^a-zA-Z0-9]", ""))