# ĐỒ ÁN CUỐI KỲ: HỆ SINH THÁI DỮ LIỆU LỚN (BIG DATA ECOSYSTEM)

## Đề tài: Xây Dựng Cụm Big Data Phân Tán Đa Node (Hadoop, YARN, Hive, Spark) trên Docker và Cài Đặt 4 Thuật Toán MapReduce Bằng PySpark

---

## 👥 1. BẢNG PHÂN CÔNG NHIỆM VỤ VÀ ĐÁNH GIÁ (TEAM ROLES & CONTRIBUTIONS)

|     STT     | Họ và Tên                                       | Vai trò / Node Cluster                   | Nhiệm vụ chính được phân công                                                                                                                                                                                                                   | Mức độ hoàn thành |
| :---------: | :------------------------------------------------- | :---------------------------------------- | :------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | :--------------------: |
| **1** | **Nguyễn Đức Toàn** *(Trưởng nhóm)* | **Master Node** (`toan-master`)   | - Thiết kế kiến trúc cụm Docker Multi-node (5 container).- Cấu hình NameNode, ResourceManager, HDFS & YARN Core.- Thiết lập Apache Hive Warehouse kết nối PostgreSQL Metastore.- Viết bài toán nâng cao PySpark DataFrame SQL Analytics. |     **100%**     |
| **2** | **Lê Tuấn Anh**                            | **Worker Node 1** (`anh-slave1`)  | - Cấu hình DataNode & NodeManager 1.- Viết mã nguồn & thực thi thuật toán **Bài toán 1: WordCount** bằng PySpark trên YARN.- Kiểm tra phân vùng dữ liệu và tổng hợp tần suất từ.                                           |     **100%**     |
| **3** | **Phạm Quốc Huy**                          | **Worker Node 2** (`huy-slave2`)  | - Cấu hình DataNode & NodeManager 2.- Viết mã nguồn & thực thi **Bài toán 2: WordMean** (độ dài trung bình của từ).- Viết mã nguồn & thực thi **Bài toán 3: WordMedian** (trung vị độ dài từ).                     |     **100%**     |
| **4** | **Phạm Đức Khoa**                         | **Worker Node 3** (`khoa-slave3`) | - Cấu hình DataNode & NodeManager 3.- Viết mã nguồn & thực thi **Bài toán 4: WordStandardDeviation** (độ lệch chuẩn độ dài từ).- Đo đạc hiệu năng phân tán MapReduce và đối chiếu kết quả.                           |     **100%**     |

---

## 🏗️ 2. KIẾN TRÚC HỆ THỐNG CỤM DOCKER MULTI-NODE

Cụm phân tán hoạt động trên mạng nội bộ Docker `bigdata_net` (`172.28.0.0/16`) gồm 5 container chuyên biệt:

```
                            +-----------------------------------+
                            |        postgres-metastore         |
                            |       IP: 172.28.0.6 (PostgreSQL) |
                            +-----------------+-----------------+
                                              | (JDBC Port 5432)
                                              v
+-------------------------------------------------------------------------------------------------+
|                                           toan-master                                           |
|                                       IP: 172.28.0.2 (Master)                                   |
|   - HDFS NameNode (9870, 9000)                      - YARN ResourceManager (8088, 8032)          |
|   - HDFS SecondaryNameNode (9868)                   - MapReduce JobHistoryServer (19888, 10020) |
|   - Hive Metastore Service (9083)                   - HiveServer2 (10000, Web UI: 10002)       |
|   - Spark Master & Client Submit Driver             - SSH Key Master Manager                    |
+-------------------+-----------------------------+-----------------------------+-----------------+
                    |                             |                             |
                    | (Data Replication 3)        | (Data Replication 3)        | (Data Replication 3)
                    v                             v                             v
   +---------------------------------+  +---------------------------------+  +---------------------------------+
   |           anh-slave1            |  |           huy-slave2            |  |           khoa-slave3           |
   |      IP: 172.28.0.3 (Worker)    |  |      IP: 172.28.0.4 (Worker)    |  |      IP: 172.28.0.5 (Worker)    |
   | - HDFS DataNode (9866)          |  | - HDFS DataNode (9866)          |  | - HDFS DataNode (9866)          |
   | - YARN NodeManager (8042)       |  | - YARN NodeManager (8042)       |  | - YARN NodeManager (8042)       |
   | - MapReduce / Spark Container   |  | - MapReduce / Spark Container   |  | - MapReduce / Spark Container   |
   +---------------------------------+  +---------------------------------+  +---------------------------------+
```

### 🌐 Danh mục Cổng & Web UI Giám sát Hệ thống:

| Tên Dịch Vụ                 | Địa chỉ Web UI                               | Chức năng minh chứng báo cáo                                                   |
| :----------------------------- | :---------------------------------------------- | :---------------------------------------------------------------------------------- |
| **HDFS NameNode**        | [http://localhost:9870](http://localhost:9870)   | Xem dung lượng cụm, 3 DataNode Live, duyệt cây thư mục HDFS Explorer.        |
| **YARN ResourceManager** | [http://localhost:8088](http://localhost:8088)   | Xem tiến độ các ứng dụng MapReduce & Spark đang chạy phân tán trên cụm. |
| **MapReduce JobHistory** | [http://localhost:19888](http://localhost:19888) | Xem chi tiết số lượng Mapper, Reducer, Node thực thi từng task.               |
| **HiveServer2 Web UI**   | [http://localhost:10002](http://localhost:10002) | Giám sát các truy vấn SQL đang kết nối qua HiveServer2.                      |

---

## 📁 3. CẤU TRÚC THƯ MỤC DỰ ÁN

```text
Bigdata-project/
├── Dockerfile                  # Base Image Ubuntu 22.04, Java 8, Hadoop 3.3.6, Spark 3.5.8, Hive 3.1.3
├── docker-compose.yml          # Thiết lập cấu hình 5 container, IP tĩnh và mount volume
├── README.md                   # Tài liệu hướng dẫn đồ án chi tiết
│
├── config/                     # Toàn bộ cấu hình cụm Big Data
│   ├── core-site.xml           # Cấu hình fs.defaultFS hdfs://toan-master:9000
│   ├── hdfs-site.xml           # dfs.replication=3, thư mục metadata và dataNode
│   ├── yarn-site.xml           # ResourceManager toan-master, classpath, shuffle service
│   ├── mapred-site.xml         # mapreduce.framework.name=yarn, AM environment HADOOP_MAPRED_HOME
│   ├── hive-site.xml           # Cấu hình Hive Metastore kết nối PostgreSQL
│   ├── spark-defaults.conf     # Cấu hình Spark chạy trên YARN cluster
│   └── workers                 # Danh sách 3 node worker (anh-slave1, huy-slave2, khoa-slave3)
│
├── data/                       # Dữ liệu nguồn
│   ├── input.txt               # Tập dữ liệu văn bản phục vụ 4 bài toán MapReduce
│   └── sales_data.csv          # Dữ liệu bán hàng bán lẻ cho Hive và PySpark SQL
│
├── scripts/                    # Các script tự động hóa quản trị cụm
│   ├── entrypoint.sh           # Khởi động SSH và giữ container chạy nền
│   ├── start-hadoop.sh         # Khởi động HDFS, YARN và JobHistoryServer
│   ├── start-hive.sh           # Khởi động dịch vụ Hive Metastore và HiveServer2
│   └── upload-data-to-hdfs.sh  # Tạo thư mục /data_input và đẩy file lên HDFS
│
└── pyspark_jobs/               # Thư mục mã nguồn 4 bài toán PySpark MapReduce
    ├── 01_workcount.py         # [Lê Tuấn Anh] Đếm tần suất xuất hiện của từ
    ├── 02_wordmean.py          # [Phạm Quốc Huy] Tính độ dài trung bình của từ
    ├── 03_workmedian.py        # [Phạm Quốc Huy] Tính trung vị độ dài từ
    ├── 04_workstd.py           # [Phạm Đức Khoa] Tính độ lệch chuẩn độ dài từ
    └── 05_ecommerce_analytics.py # [Nguyễn Đức Toàn] Phân tích thương mại điện tử với Spark SQL
```

---

## 🚀 4. HƯỚNG DẪN VẬN HÀNH CỤM (QUICKSTART)

### Bước 1: Khởi động toàn bộ container

Mở PowerShell tại thư mục dự án `e:\Bigdata-project`:

```powershell
docker compose up -d
```

Kiểm tra trạng thái các container:

```powershell
docker compose ps
```

*(Cần đảm bảo 5 container: `postgres-metastore`, `toan-master`, `anh-slave1`, `huy-slave2`, `khoa-slave3` đều ở trạng thái `Up`).*

---

### Bước 2: Đăng nhập vào node Master

```powershell
docker exec -it toan-master bash
```

---

### Bước 3: Khởi động các dịch vụ Hadoop & Hive (Bằng Script tự động)

Trong terminal của `toan-master`, chạy 3 script tự động có sẵn:

```bash
# 1. Khởi động HDFS + YARN + JobHistoryServer
bash /home/hadoop/scripts/start-hadoop.sh

# 2. Khởi động Hive Metastore + HiveServer2
bash /home/hadoop/scripts/start-hive.sh

# 3. Nạp dữ liệu vào HDFS (/data_input/input.txt và sales_data.csv)
bash /home/hadoop/scripts/upload-data-to-hdfs.sh
```

Kiểm tra tiến trình Java bằng lệnh `jps`, trên `toan-master` phải có đủ:

- `NameNode`
- `SecondaryNameNode`
- `ResourceManager`
- `JobHistoryServer`
- `RunJar` (Hive Metastore)
- `RunJar` (HiveServer2)

---
