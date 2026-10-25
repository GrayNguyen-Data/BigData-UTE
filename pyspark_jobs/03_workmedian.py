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
        .appName("03_WordMedian_MapReduce") \
        .getOrCreate()

    sc = spark.sparkContext
    sc.setLogLevel("WARN")

    cli_path = sys.argv[1] if len(sys.argv) > 1 else None
    input_path = resolve_input_path(spark, cli_path)

    print("=" * 65)
    print(" BÀI TOÁN 3: TÍNH TRUNG VỊ ĐỘ DÀI CỦA TỪ (WORD MEDIAN)")
    print("=" * 65)
    print(f">> Đang đọc dữ liệu từ: {input_path}")

    lines_rdd = sc.textFile(input_path)
    words_rdd = lines_rdd.flatMap(clean_and_split)

    sorted_lengths_rdd = words_rdd.map(lambda word: len(word)).sortBy(lambda x: x, ascending=True)

    indexed_lengths_rdd = sorted_lengths_rdd.zipWithIndex().map(lambda pair: (pair[1], pair[0])).cache()

    total_words = indexed_lengths_rdd.count()

    if total_words == 0:
        median_length = 0.0
        mid_info = "Không có dữ liệu"
    elif total_words % 2 == 1:
        mid_idx = (total_words - 1) // 2
        val = indexed_lengths_rdd.lookup(mid_idx)[0]
        median_length = float(val)
        mid_info = f"N lẻ -> Phần tử tại index {mid_idx}: {val}"
    else:
        mid_idx1 = total_words // 2 - 1
        mid_idx2 = total_words // 2
        val1 = indexed_lengths_rdd.lookup(mid_idx1)[0]
        val2 = indexed_lengths_rdd.lookup(mid_idx2)[0]
        median_length = (val1 + val2) / 2.0
        mid_info = f"N chẵn -> Trung bình 2 phần tử tại index {mid_idx1} ({val1}) và {mid_idx2} ({val2})"

    length_distribution = (
        words_rdd.map(lambda w: (len(w), 1))
        .reduceByKey(lambda a, b: a + b)
        .sortByKey(ascending=True)
        .collect()
    )

    min_len = length_distribution[0][0] if length_distribution else 0
    max_len = length_distribution[-1][0] if length_distribution else 0

    print("-" * 65)
    print(f">> Tổng số từ (N)                  : {total_words}")
    print(f">> Độ dài nhỏ nhất / lớn nhất      : {min_len} / {max_len}")
    print(f">> Vị trí trung tâm                : {mid_info}")
    print(f">> Trung vị độ dài từ (Word Median): {median_length:.4f}")
    print("-" * 65)
    print(">> Bảng phân phối tần suất & tần suất tích lũy:")
    print(f"{'Độ dài':<10} | {'Số lượng từ':<15} | {'Tích lũy':<15} | {'Tỷ lệ tích lũy':<15}")
    print("-" * 65)
    cumulative = 0
    for length, count in length_distribution:
        cumulative += count
        cum_pct = (cumulative / total_words) * 100 if total_words > 0 else 0.0
        marker = " <-- Median" if (cumulative - count < (total_words + 1) / 2 <= cumulative) else ""
        print(f"{length:<10} | {count:<15} | {cumulative:<15} | {cum_pct:>12.2f}%{marker}")
    print("=" * 65)

    spark.stop()


if __name__ == "__main__":
    main()
