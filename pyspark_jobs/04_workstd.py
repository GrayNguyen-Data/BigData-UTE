"""Calculate the population standard deviation of word lengths with PySpark RDDs."""

import argparse
import math
import re
import time

from pyspark import SparkContext


def words_in_line(line):
    # Keep Unicode letters and digits; punctuation, underscores and hyphens
    # separate words. Use the same rule for WordMean/WordMedian comparisons.
    return re.findall(r"[^\W_]+", line, flags=re.UNICODE)


def add_pairs(left, right):
    return left[0] + right[0], left[1] + right[1]


def main():
    parser = argparse.ArgumentParser(description="Word length standard deviation")
    parser.add_argument(
        "--input",
        default="hdfs://toan-master:9000/data_input/input.txt",
        help="HDFS or file:// URI of the input text",
    )
    args = parser.parse_args()

    sc = SparkContext(appName="WordStandardDeviation-Khoa")
    sc.setLogLevel("WARN")
    try:
        lengths = sc.textFile(args.input).flatMap(words_in_line).map(len).cache()

        # Pass 1: sum lengths and count words across all partitions.
        pass_one_started = time.perf_counter()
        total_length, word_count = lengths.map(lambda length: (length, 1)).fold(
            (0, 0), add_pairs
        )
        pass_one_seconds = time.perf_counter() - pass_one_started
        if word_count == 0:
            raise ValueError("Input khong co tu nao de tinh do lech chuan")

        mean = total_length / word_count

        # Pass 2: sum squared distances to the mean; divide by N (population).
        pass_two_started = time.perf_counter()
        sum_squared = lengths.map(lambda length: (length - mean) ** 2).fold(
            0.0, lambda left, right: left + right
        )
        pass_two_seconds = time.perf_counter() - pass_two_started
        variance = sum_squared / word_count
        stddev = math.sqrt(variance)

        print("So phan vung: {}".format(lengths.getNumPartitions()))
        print("So tu: {}".format(word_count))
        print("Do dai trung binh: {:.6f}".format(mean))
        print("Phuong sai tong the: {:.6f}".format(variance))
        print("Do lech chuan tong the: {:.6f}".format(stddev))
        print("Thoi gian pass 1: {:.3f} giay".format(pass_one_seconds))
        print("Thoi gian pass 2: {:.3f} giay".format(pass_two_seconds))
        print(
            "Tong thoi gian xu ly RDD: {:.3f} giay".format(
                pass_one_seconds + pass_two_seconds
            )
        )
    finally:
        sc.stop()


if __name__ == "__main__":
    main()
