#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Bài toán 5 (Nâng cao): E-commerce Retail Analytics với PySpark DataFrame & Spark SQL
Mục tiêu: Đọc dữ liệu bán lẻ sales_data.csv từ HDFS/Local, đăng ký TempView và thực thi
các câu truy vấn phân tích doanh thu đa chiều
"""

import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, count, avg, round, desc


def resolve_sales_data_path(spark, cli_arg=None):
    if cli_arg:
        return cli_arg

    hdfs_path = "hdfs://toan-master:9000/data_input/sales_data.csv"
    local_candidates = [
        os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data", "sales_data.csv")),
        os.path.abspath("data/sales_data.csv"),
        "/home/hadoop/shared_data/sales_data.csv",
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
        .appName("05_EcommerceAnalytics_Toan") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    cli_path = sys.argv[1] if len(sys.argv) > 1 else None
    input_path = resolve_sales_data_path(spark, cli_path)

    print("=" * 75)
    print(" BÀI TOÁN 5 (NÂNG CAO): PHÂN TÍCH DOANH THU E-COMMERCE VỚI SPARK SQL")
    print(" Phụ trách: Nguyễn Đức Toàn (Master Node)")
    print("=" * 75)
    print(f">> Nguồn dữ liệu: {input_path}\n")

    # 1. Đọc dữ liệu CSV vào DataFrame
    df = spark.read.csv(
        input_path,
        header=True,
        inferSchema=True
    )

    print(">> Schema dữ liệu bán lẻ (Schema Inference):")
    df.printSchema()

    # Tạo cột thành tiền = quantity * price
    df = df.withColumn("total_amount", round(col("quantity") * col("price"), 2))

    # Đăng ký bảng tạm để dùng Spark SQL
    df.createOrReplaceTempView("sales")

    print("\n" + "=" * 75)
    print(" TRUY VẤN 1: TỔNG DOANH THU & ĐƠN GIÁ TRUNG BÌNH THEO DANH MỤC (CATEGORY)")
    print("=" * 75)
    query1 = spark.sql("""
        SELECT 
            category,
            COUNT(*) AS total_orders,
            SUM(quantity) AS total_items_sold,
            ROUND(SUM(total_amount), 2) AS total_revenue,
            ROUND(AVG(price), 2) AS avg_unit_price
        FROM sales
        GROUP BY category
        ORDER BY total_revenue DESC
    """)
    query1.show(truncate=False)

    print("=" * 75)
    print(" TRUY VẤN 2: TOP 3 THÀNH PHỐ CÓ DOANH THU CAO NHẤT (CITY BREAKDOWN)")
    print("=" * 75)
    query2 = spark.sql("""
        SELECT 
            city,
            COUNT(DISTINCT order_id) AS orders_count,
            ROUND(SUM(total_amount), 2) AS city_revenue
        FROM sales
        GROUP BY city
        ORDER BY city_revenue DESC
        LIMIT 3
    """)
    query2.show(truncate=False)

    print("=" * 75)
    print(" TRUY VẤN 3: TOP 5 SẢN PHẨM MANG LẠI DOANH THU LỚN NHẤT")
    print("=" * 75)
    query3 = spark.sql("""
        SELECT 
            product,
            category,
            quantity,
            price,
            total_amount
        FROM sales
        ORDER BY total_amount DESC
        LIMIT 5
    """)
    query3.show(truncate=False)

    print("=" * 75)
    print(" TỔNG KẾT SO SÁNH VỚI APACHE HIVE:")
    print(" - Hive: Mất ~90 giây (dịch sang MapReduce cổ điển, ghi ra disk)")
    print(" - Spark SQL: Hoàn tất trong vài giây nhờ In-Memory RDD & Catalyst Optimizer")
    print("=" * 75 + "\n")

    spark.stop()


if __name__ == "__main__":
    main()
