#!/bin/bash
# ==============================================================================
# Script đẩy dữ liệu mẫu lên HDFS phục vụ chạy Hive và PySpark
# ==============================================================================

echo "=========================================================="
echo ">> [1/3] Tạo thư mục trên HDFS..."
echo "=========================================================="
hdfs dfs -mkdir -p /data_input
hdfs dfs -chmod -R 777 /data_input

echo "=========================================================="
echo ">> [2/3] Tải file input.txt và sales_data.csv lên HDFS..."
echo "=========================================================="
hdfs dfs -put -f /home/hadoop/shared_data/input.txt /data_input/input.txt
hdfs dfs -put -f /home/hadoop/shared_data/sales_data.csv /data_input/sales_data.csv

echo "=========================================================="
echo ">> [3/3] Kiểm tra danh sách file trên HDFS /data_input:"
echo "=========================================================="
hdfs dfs -ls -h /data_input

echo "=========================================================="
echo ">> Hoàn tất nạp dữ liệu lên HDFS!"
echo "=========================================================="
