#!/bin/bash
# ==============================================================================
# Script khởi động toàn bộ dịch vụ Hadoop (HDFS, YARN, MapReduce HistoryServer)
# Sử dụng trên toan-master
# ==============================================================================

echo "=========================================================="
echo ">> [1/3] Khởi động Hadoop Distributed File System (HDFS)..."
echo "=========================================================="
start-dfs.sh

echo "=========================================================="
echo ">> [2/3] Khởi động YARN Cluster Resource Manager & Workers..."
echo "=========================================================="
start-yarn.sh

echo "=========================================================="
echo ">> [3/3] Khởi động MapReduce JobHistoryServer..."
echo "=========================================================="
mapred --daemon start historyserver

echo "=========================================================="
echo ">> Kiểm tra danh sách tiến trình Java (JPS) trên toan-master:"
echo "=========================================================="
jps

echo ""
echo ">> Hadoop Cluster đã sẵn sàng!"
echo "   - HDFS NameNode Web UI:       http://localhost:9870"
echo "   - YARN ResourceManager UI:    http://localhost:8088"
echo "   - MapReduce History Web UI:   http://localhost:19888"
echo "=========================================================="
