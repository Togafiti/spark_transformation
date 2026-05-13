import json
import os
from typing import List

from airflow.models import Variable


def read_setting(name: str, default: str = "") -> str:
    # Đọc từ Airflow Variable trước để DAG chạy được trên Airflow và Docker
    # mà không cần đổi code; fallback giữ local/dev run vẫn dùng được.
    value = Variable.get(name, default_var=os.getenv(name, default))
    return (value or "").strip()


def build_spark_command(script_path: str) -> List[str]:
    # Xây dựng lệnh spark-submit với packages và settings được cấu hình.
    
    # Mỗi DAG cần chạy spark-submit với cùng một cách giải quyết dependency
    # và cấu hình Hadoop để tránh xung đột giữa các DAG.
    spark_submit_bin = read_setting("SPARK_SUBMIT_BIN", "spark-submit")
    spark_master = read_setting("SPARK_MASTER", "")
    spark_packages = read_setting(
        "SPARK_PACKAGES",
        "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262",
    )

    command = [spark_submit_bin]
    if spark_master:
        command.extend(["--master", spark_master])
    if spark_packages:
        # Spark cần Hadoop AWS jars lúc submit để nói chuyện với s3a:// paths.
        command.extend(["--packages", spark_packages])

    extra_conf_raw = read_setting("SPARK_EXTRA_CONF", "")
    if extra_conf_raw:
        extra_conf = json.loads(extra_conf_raw)
        for key, value in extra_conf.items():
            command.extend(["--conf", f"{key}={value}"])

    command.append(script_path)
    return command
