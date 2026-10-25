#!/bin/bash

# 1. Khởi động dịch vụ SSH server ngầm để các node giao tiếp được với nhau
sudo service ssh start

# Đảm bảo PATH luôn có đầy đủ lệnh hadoop/spark/hive cho mọi phiên shell
if ! grep -q "HADOOP_HOME" /home/hadoop/.bashrc 2>/dev/null; then
    echo 'export PATH=$PATH:/opt/hadoop/bin:/opt/hadoop/sbin:/opt/spark/bin:/opt/spark/sbin:/opt/hive/bin' >> /home/hadoop/.bashrc
fi

echo "=========================================================="
echo " Node [$(hostname)] role: ${NODE_ROLE:-worker} is ready! "
echo "=========================================================="

# 2. Nếu là Master: tự động bật Hadoop và Hive nếu NameNode đã được format
if [ "${NODE_ROLE}" = "master" ]; then
    (
        # Chờ 6 giây để 3 container worker và postgres khởi động xong SSH & DB
        sleep 6
        if [ -d "/home/hadoop/hdfs/nameNode/current" ]; then
            echo ">> [AUTO-START] Đã phát hiện NameNode format. Tự động bật cụm..."
            bash /home/hadoop/scripts/start-hadoop.sh
            bash /home/hadoop/scripts/start-hive.sh
            echo ">> [AUTO-START] Tất cả dịch vụ Hadoop & Hive đã sẵn sàng!"
        else
            echo ">> [INFO] NameNode chưa được format. Vui lòng tự format bằng: hdfs namenode -format"
        fi
    ) &
fi

# 3. LỆNH SỐNG CÒN CỦA DOCKER: Giữ tiến trình foreground để container luôn chạy
tail -f /dev/null
