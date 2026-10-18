# 1. Base OS: Ubuntu Server 22.04 LTS theo đúng chuẩn đề bài
FROM ubuntu:22.04

LABEL maintainer="BigData-Team: Toan(Master), Anh(Slave1), Huy(Slave2), Khoa(Slave3)"

# Tự động hóa các prompt cài đặt, tránh bị treo chờ nhập phím
ENV DEBIAN_FRONTEND=noninteractive

# 2. Cài đặt các gói phụ thuộc cơ sở: Java OpenJDK 8, SSH Server, Python 3, mạng
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-8-jdk \
    openssh-server \
    openssh-client \
    rsync \
    curl \
    tar \
    nano \
    vim \
    net-tools \
    iputils-ping \
    python3 \
    python3-pip \
    sudo \
    && rm -rf /var/lib/apt/lists/*

# Cài đặt thư viện Python hỗ trợ thuật toán PySpark (Mean, Median, Std)
RUN pip3 install --no-cache-dir numpy

# Thiết lập biến môi trường JAVA_HOME chuẩn
ENV JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

# 3. Tạo user quản trị hệ thống "hadoop" với quyền sudo không cần gõ mật khẩu
RUN useradd -m -s /bin/bash hadoop && \
    echo "hadoop:hadoop" | chpasswd && \
    usermod -aG sudo hadoop && \
    echo "hadoop ALL=(ALL) NOPASSWD:ALL" >> /etc/sudoers

# 4. Tải và cài đặt Apache Hadoop 3.3.6 qua Apache DLCdn siêu tốc
ENV HADOOP_VERSION=3.3.6
ENV HADOOP_HOME=/opt/hadoop
RUN curl -fSL --retry 5 --retry-delay 3 "https://dlcdn.apache.org/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz" -o /tmp/hadoop.tar.gz && \
    tar -xzf /tmp/hadoop.tar.gz -C /opt && \
    mv /opt/hadoop-${HADOOP_VERSION} ${HADOOP_HOME} && \
    rm /tmp/hadoop.tar.gz

# 5. Tải và cài đặt Apache Spark 3.5.8 (Bản mới nhất và có sẵn trên DLCdn)
ENV SPARK_VERSION=3.5.8
ENV SPARK_HOME=/opt/spark
RUN curl -fSL --retry 5 --retry-delay 3 "https://dlcdn.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz" -o /tmp/spark.tar.gz && \
    tar -xzf /tmp/spark.tar.gz -C /opt && \
    mv /opt/spark-${SPARK_VERSION}-bin-hadoop3 ${SPARK_HOME} && \
    rm /tmp/spark.tar.gz

# 6. Tải và cài đặt Apache Hive 3.1.3 (Chuẩn tương thích hoàn hảo Java 8)
ENV HIVE_VERSION=3.1.3
ENV HIVE_HOME=/opt/hive
RUN curl -fSL --retry 5 --retry-delay 3 "https://repo.huaweicloud.com/apache/hive/hive-${HIVE_VERSION}/apache-hive-${HIVE_VERSION}-bin.tar.gz" -o /tmp/hive.tar.gz && \
    tar -xzf /tmp/hive.tar.gz -C /opt && \
    mv /opt/apache-hive-${HIVE_VERSION}-bin ${HIVE_HOME} && \
    rm /tmp/hive.tar.gz

# Tải PostgreSQL JDBC Driver cho Hive Metastore
RUN curl -fSL --retry 5 "https://jdbc.postgresql.org/download/postgresql-42.7.3.jar" -o ${HIVE_HOME}/lib/postgresql-42.7.3.jar

# Fix lỗi xung đột thư viện guava giữa Hive 3.1.3 và Hadoop 3.3.x
RUN rm -f ${HIVE_HOME}/lib/guava-19.0.jar && \
    cp ${HADOOP_HOME}/share/hadoop/hdfs/lib/guava-*.jar ${HIVE_HOME}/lib/

# Fix lỗi xung đột thư viện guava giữa Hive 3.1.3 và Hadoop 3.3.x
RUN rm -f ${HIVE_HOME}/lib/guava-19.0.jar && \
    cp ${HADOOP_HOME}/share/hadoop/hdfs/lib/guava-*.jar ${HIVE_HOME}/lib/

# 7. Khai báo biến môi trường toàn cục cho Hadoop, Spark, Hive vào PATH
ENV HADOOP_CONF_DIR=${HADOOP_HOME}/etc/hadoop
ENV YARN_CONF_DIR=${HADOOP_HOME}/etc/hadoop
ENV SPARK_CONF_DIR=${SPARK_HOME}/conf
ENV PATH=${PATH}:${HADOOP_HOME}/bin:${HADOOP_HOME}/sbin:${SPARK_HOME}/bin:${SPARK_HOME}/sbin:${HIVE_HOME}/bin
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

# Khai báo JAVA_HOME trong hadoop-env.sh
RUN echo "export JAVA_HOME=${JAVA_HOME}" >> ${HADOOP_HOME}/etc/hadoop/hadoop-env.sh

# 8. Cấu hình SSH Passwordless cho user hadoop
USER hadoop
WORKDIR /home/hadoop
RUN ssh-keygen -t rsa -P '' -f /home/hadoop/.ssh/id_rsa && \
    cat /home/hadoop/.ssh/id_rsa.pub >> /home/hadoop/.ssh/authorized_keys && \
    chmod 0600 /home/hadoop/.ssh/authorized_keys

# Tự động accept host keys (không dừng lại hỏi yes/no khi master ssh sang slaves)
RUN echo "Host *\n  StrictHostKeyChecking no\n  UserKnownHostsFile /dev/null" >> /home/hadoop/.ssh/config && \
    chmod 0600 /home/hadoop/.ssh/config

# 9. Tạo thư mục lưu trữ HDFS và phân quyền cho user hadoop
USER root
RUN mkdir -p /home/hadoop/hdfs/nameNode && \
    mkdir -p /home/hadoop/hdfs/dataNode && \
    mkdir -p /home/hadoop/hdfs/temp && \
    chown -R hadoop:hadoop /home/hadoop/hdfs ${HADOOP_HOME} ${SPARK_HOME} ${HIVE_HOME}

# 10. Copy các script khởi động và cấp quyền thực thi
COPY --chown=hadoop:hadoop scripts/ /home/hadoop/scripts/
RUN chmod +x /home/hadoop/scripts/*.sh

# Mặc định container chạy dưới quyền user hadoop
USER hadoop
WORKDIR /home/hadoop

# 11. Khai báo các port dịch vụ của cụm
EXPOSE 22 9870 9000 8088 8042 19888 10000 10002 9083 4040 8080 7077

# Khởi động dịch vụ SSH thông qua entrypoint script
ENTRYPOINT ["/bin/bash", "/home/hadoop/scripts/entrypoint.sh"]
