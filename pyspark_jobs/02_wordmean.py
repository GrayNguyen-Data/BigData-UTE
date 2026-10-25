#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import re
import sys
from pyspark.sql import SparkSession


def clean_and_split(line):
    words = re.findall(r"[a-zA-Z0-9]+", line.lower())
    return [w for w in words if len(w) > 0]


def resolve_input_path(spark, cli_arg=None):
    if cli_arg:
        return cli_arg

    hdfs_path = "hdfs://toan-master:9000/data_input/input.txt"
    local_candidates = [
        os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data", "input.txt")),
        os.path.abspath("data/input.txt"),
        "/home/hadoop/shared_data/input.txt",
    ]

    try:
        sc = spark.sparkContext
        uri = sc._jvm.java.net.URI(hdfs_path)
        fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(uri, sc._jsc.hadoopConfiguration())
        if fs.exists(sc._jvm.org.apache.hadoop.fs.Path(hdfs_path)):
            return hdfs_path
    except Exception:
        pass

    for path in local_candidates:
        if os.path.exists(path):
            return f"file://{path}"

    return hdfs_path


def main():
    spark = SparkSession.builder \
        .appName("02_WordMean_MapReduce") \
        .getOrCreate()

    sc = spark.sparkContext
    sc.setLogLevel("WARN")

    cli_path = sys.argv[1] if len(sys.argv) > 1 else None
    input_path = resolve_input_path(spark, cli_path)

    print("=" * 65)
    print(" BÀI TOÁN 2: TÍNH ĐỘ DÀI TRUNG BÌNH CỦA TỪ (WORD MEAN)")
    print("=" * 65)
    print(f">> Đang đọc dữ liệu từ: {input_path}")

    lines_rdd = sc.textFile(input_path)

    words_rdd = lines_rdd.flatMap(clean_and_split)

    word_lengths_rdd = words_rdd.map(lambda word: (len(word), 1))

    total_length, total_words = word_lengths_rdd.reduce(
        lambda a, b: (a[0] + b[0], a[1] + b[1])
    )

    mean_length = total_length / total_words if total_words > 0 else 0.0

    length_distribution = (
        words_rdd.map(lambda w: (len(w), 1))
        .reduceByKey(lambda a, b: a + b)
        .sortByKey(ascending=True)
        .collect()
    )

    print("-" * 65)
    print(f">> Tổng số từ (N)                : {total_words}")
    print(f">> Tổng độ dài các từ (Sum L)    : {total_length}")
    print(f">> Độ dài trung bình (Word Mean) : {mean_length:.4f}")
    print("-" * 65)
    print(">> Bảng phân phối độ dài từ:")
    print(f"{'Độ dài (ký tự)':<20} | {'Số lượng từ':<15} | {'Tỷ lệ (%)':<10}")
    print("-" * 65)
    for length, count in length_distribution:
        pct = (count / total_words) * 100 if total_words > 0 else 0.0
        print(f"{length:<20} | {count:<15} | {pct:>8.2f}%")
    print("=" * 65)

    spark.stop()


if __name__ == "__main__":
    main()
