#!/bin/bash
# ==============================================================================
# Script khởi động Hive Metastore Service & HiveServer2 trên toan-master
# ==============================================================================

echo "=========================================================="
echo ">> [1/2] Khởi động Hive Metastore Thrift Service (Port 9083)..."
echo "=========================================================="
nohup hive --service metastore > /home/hadoop/hive-metastore.log 2>&1 &
sleep 3

echo "=========================================================="
echo ">> [2/2] Khởi động HiveServer2 (Thrift 10000 / Web UI 10002)..."
echo "=========================================================="
nohup hive --service hiveserver2 > /home/hadoop/hiveserver2.log 2>&1 &
sleep 5

echo "=========================================================="
echo ">> Tiến trình Java hiện tại (JPS):"
echo "=========================================================="
jps

echo ""
echo ">> Dịch vụ Apache Hive đã chạy nền:"
echo "   - Hive Metastore: Cổng 9083"
echo "   - HiveServer2:    Cổng 10000 (JDBC) / Web UI: http://localhost:10002"
echo "   - Log Metastore:  /home/hadoop/hive-metastore.log"
echo "   - Log Server2:    /home/hadoop/hiveserver2.log"
echo "=========================================================="
