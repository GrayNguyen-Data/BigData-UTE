# BÁO CÁO CUỐI KỲ MÔN NHẬP MÔN DỮ LIỆU LỚN

# ĐỀ TÀI: XÂY DỰNG CỤM BIG DATA PHÂN TÁN MULTI-NODE (HADOOP, YARN, HIVE, SPARK) TRÊN DOCKER VÀ THỰC THI 4 THUẬT TOÁN MAPREDUCE BẰNG PYSPARK

> **Lưu ý rà soát ngày 22/10/2026:** Đây là bản thảo có nội dung kết quả cần đối chiếu lại. Năm file trong `pyspark_jobs/` hiện đang rỗng, vì vậy chưa thể xác nhận các thuật toán PySpark chạy được. Các job ID, thời gian chạy, trạng thái hoàn thành và kết quả bên dưới chỉ nên giữ nếu nhóm còn log/ảnh minh chứng từ lần chạy thực tế. Tham khảo [khung báo cáo và ôn tập](./KHUNG_BAO_CAO_VA_ON_TAP.md) để biết các điểm cần kiểm chứng và cách phân biệt Spark trên YARN với Hadoop MapReduce.

---

## 🏛️ THÔNG TIN CHUNG

- **Trường**: Đại học Sư Phạm Kỹ Thuật TP. Hồ Chí Minh
- **Khoa**: Công nghệ Thông tin
- **Môn học**: Nhập môn Dữ liệu lớn (Big Data Essentials)
- **Học kỳ**: HKI (2026 - 2027)
- **Nhóm thực hiện**: Nhóm Toàn - Anh - Huy - Khoa

---

## 👥 BẢNG PHÂN CÔNG NHIỆM VỤ VÀ ĐÁNH GIÁ (100% HOÀN THÀNH)

|     STT     | Họ và Tên                                       | Vai trò / Node Cluster                   | Nhiệm vụ chi tiết được phân công                                                                                                                                                                                                                                                                                                                           | Đánh giá hoàn thành |
| :---------: | :------------------------------------------------- | :---------------------------------------- | :----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | :----------------------: |
| **1** | **Nguyễn Đức Toàn** *(Trưởng nhóm)* | **Master Node** (`toan-master`)   | - Thiết kế kiến trúc mạng ảo Docker `bigdata_net` và cụm 5 container.- Cấu hình NameNode, SecondaryNameNode, ResourceManager, JobHistoryServer.- Thiết lập Apache Hive Warehouse kết nối PostgreSQL Metastore.- Thực thi kịch bản Data Warehouse phân tích doanh thu bán lẻ.- Phân tích nâng cao bằng PySpark DataFrame SQL trên YARN. |      **100%**      |
| **2** | **Lê Tuấn Anh**                            | **Worker Node 1** (`anh-slave1`)  | - Cấu hình DataNode 1 & NodeManager 1.- Viết mã nguồn & thực thi **Bài toán 1: WordCount** bằng PySpark trên YARN.- Kiểm tra phân vùng dữ liệu và đo lường tần suất từ.                                                                                                                                                                |      **100%**      |
| **3** | **Phạm Quốc Huy**                          | **Worker Node 2** (`huy-slave2`)  | - Cấu hình DataNode 2 & NodeManager 2.- Viết mã nguồn & thực thi **Bài toán 2: WordMean** (độ dài trung bình của từ).- Viết mã nguồn & thực thi **Bài toán 3: WordMedian** (trung vị độ dài của từ).                                                                                                                         |      **100%**      |
| **4** | **Phạm Đức Khoa**                         | **Worker Node 3** (`khoa-slave3`) | - Cấu hình DataNode 3 & NodeManager 3.- Viết mã nguồn & thực thi **Bài toán 4: WordStandardDeviation** (độ lệch chuẩn độ dài từ).- Đo đạc hiệu năng phân tán MapReduce và kiểm tra tính chịu lỗi của cụm.                                                                                                                      |      **100%**      |

---

# PHẦN 1: CƠ SỞ LÝ THUYẾT HỆ SINH THÁI BIG DATA

## 1.1. Apache Hadoop

Apache Hadoop là một nền tảng mã nguồn mở (Apache Software Foundation) cho phép lưu trữ và xử lý phân tán các tập dữ liệu cực lớn (Big Data) trên các cụm máy tính phổ thông (Commodity Hardware).
Ba trụ cột cốt lõi của Hadoop:

1. **HDFS (Hadoop Distributed File System)**:
   - Lưu trữ dữ liệu dạng khối (Block-based storage), mặc định mỗi block 128 MB.
   - Áp dụng cơ chế **Replication** (nhân bản 3 bản sao trên 3 DataNode khác nhau) để đảm bảo an toàn tuyệt đối khi có node bị sập (Fault Tolerance).
   - Mô hình Master/Worker: **NameNode** quản lý metadata cây thư mục trong bộ nhớ RAM, **DataNode** trực tiếp lưu trữ các khối dữ liệu trên đĩa cứng.
2. **YARN (Yet Another Resource Negotiator)**:
   - Hệ điều hành phân tán quản lý và cấp phát tài nguyên (RAM, CPU cores).
   - **ResourceManager (Master)**: Tiếp nhận job, lập lịch và điều phối tài nguyên cụm.
   - **NodeManager (Worker)**: Giám sát tài nguyên trên từng máy slave và khởi chạy các **Container**.
   - **ApplicationMaster (AM)**: Mỗi ứng dụng khi chạy sẽ có 1 AM đại diện đàm phán tài nguyên với ResourceManager.
3. **MapReduce**:
   - Mô hình lập trình xử lý song song chia làm 2 pha: **Map** (ánh xạ, lọc, phát sinh cặp key-value) và **Reduce** (gom nhóm và tổng hợp theo key).
   - Cơ chế **Data Locality**: Map task được ưu tiên chạy ngay tại node đang chứa block dữ liệu trên HDFS để giảm tối đa chi phí truyền tải qua mạng (Network I/O).

## 1.2. Apache Hive

- Apache Hive là một hệ thống Data Warehouse phân tán được xây dựng trên nền tảng Hadoop.
- Hive cung cấp ngôn ngữ **HiveQL (HQL)** có cú pháp tương tự SQL chuẩn, giúp lập trình viên không cần viết mã Java MapReduce phức tạp mà vẫn truy vấn được hàng triệu bản ghi trên HDFS.
- **Hive Metastore**: Lưu trữ cấu trúc schema bảng (tên cột, kiểu dữ liệu, định dạng file, đường dẫn HDFS) vào hệ quản trị cơ sở dữ liệu quan hệ (RDBMS) như **PostgreSQL** hoặc MySQL.
- Khi người dùng gửi câu lệnh SQL, Hive Compiler sẽ phân tích cú pháp (AST) -> Tối ưu hóa kế hoạch thực thi (Logical/Physical Plan) -> Dịch thành các Job **MapReduce** và nộp lên YARN để thực thi song song.

## 1.3. Apache Spark & PySpark

- Apache Spark là engine tính toán phân tán thế hệ mới với khả năng **xử lý trong bộ nhớ (In-Memory Processing)**, khắc phục nhược điểm ghi đọc liên tục xuống đĩa cứng của MapReduce cổ điển (nhanh hơn từ 10 đến 100 lần).
- **RDD (Resilient Distributed Dataset)**: Cấu trúc dữ liệu phân tán có khả năng chịu lỗi, hỗ trợ 2 nhóm phép toán:
  - *Transformations* (Lazy Evaluation): `map`, `flatMap`, `filter`, `reduceByKey`, `sortBy`, `zipWithIndex`.
  - *Actions* (Thực thi và trả kết quả): `collect`, `take`, `count`, `reduce`.
- **PySpark**: Thư viện Python chính thức giao tiếp với Spark Core thông qua Py4J, kết hợp sức mạnh phân tán của Spark với sự tiện lợi của Python.

---

# PHẦN 2: THIẾT KẾ VÀ CÀI ĐẶT CỤM DOCKER MULTI-NODE

## 2.1. Kiến trúc cụm 5 Container trên mạng Docker Bridge

Hệ thống được ảo hóa bằng Docker Compose trên subnet tĩnh `172.28.0.0/16`:

```
+--------------------------------------------------------------------------------------------------+
|                                  Mạng Docker: bigdata_net (172.28.0.0/16)                        |
|                                                                                                  |
|  [postgres-metastore: 172.28.0.6] <== (JDBC Port 5432) ==                                        |
|                                                         ||                                       |
|  [toan-master: 172.28.0.2] <=============================                                        |
|   NameNode (9870), ResourceManager (8088), JobHistory (19888), HiveServer2 (10000, UI: 10002)   |
|         ||                                              ||                                       |
|         || (HDFS RPC 9000 & YARN NodeManager 8042)      ||                                       |
|         v                                               v                                        |
|  [anh-slave1: 172.28.0.3]             [huy-slave2: 172.28.0.4]          [hakhoa-slave3: 172.28.0.5]|
|   DataNode + NodeManager               DataNode + NodeManager            DataNode + NodeManager  |
+--------------------------------------------------------------------------------------------------+
```

## 2.2. Phân tích các thông số cấu hình quan trọng (Configuration Files)

### 1. `core-site.xml`

- `fs.defaultFS = hdfs://toan-master:9000`: Định danh hệ thống tệp mặc định là HDFS trỏ về node Master.
- `hadoop.proxyuser.hadoop.hosts = *`: Cấp quyền proxy cho user hadoop kết nối từ các dịch vụ ngoài (HiveServer2, Spark).

### 2. `hdfs-site.xml`

- `dfs.replication = 3`: Nhân bản dữ liệu thành 3 bản sao trải đều trên 3 node worker (`anh-slave1`, `huy-slave2`, `khoa-slave3`).
- `dfs.namenode.name.dir = file:///home/hadoop/hdfs/nameNode`: Thư mục lưu metadata trên Master.
- `dfs.datanode.data.dir = file:///home/hadoop/hdfs/dataNode`: Thư mục lưu các block thực tế trên Worker.

### 3. `yarn-site.xml`

- `yarn.resourcemanager.hostname = toan-master`: Định nghĩa máy chủ điều phối tài nguyên.
- `yarn.nodemanager.aux-services = mapreduce_shuffle`: Kích hoạt dịch vụ tráo đổi dữ liệu (Shuffle) giữa các node.
- `yarn.nodemanager.vmem-check-enabled = false`: Tắt kiểm tra bộ nhớ ảo để tránh YARN vô tình kill container chạy trong môi trường Docker.
- `yarn.application.classpath`: Khai báo toàn bộ đường dẫn thư viện JAR của Hadoop để các worker container tìm nạp class thực thi.

### 4. `mapred-site.xml`

- `mapreduce.framework.name = yarn`: Bắt buộc MapReduce phải chạy thông qua YARN điều phối.
- `yarn.app.mapreduce.am.env = HADOOP_MAPRED_HOME=/opt/hadoop`: **Cấu hình tối quan trọng trên Hadoop 3**, giải quyết triệt để lỗi `ClassNotFoundException: MRAppMaster`.

### 5. `hive-site.xml`

- `javax.jdo.option.ConnectionURL = jdbc:postgresql://postgres-metastore:5432/metastore_db`: Trỏ Metastore về database PostgreSQL độc lập.
- `hive.metastore.uris = thrift://toan-master:9083`: Cổng dịch vụ Thrift của Hive Metastore.

---

# PHẦN 3: THỰC THI TRIỂN KHAI DATA WAREHOUSE VỚI APACHE HIVE

## 3.1. Kịch bản nạp dữ liệu và tạo bảng Data Warehouse

Dữ liệu nguồn là tập tin `sales_data.csv` (chứa các thông tin giao dịch bán lẻ gồm `order_id`, `customer_name`, `product`, `category`, `quantity`, `price`, `order_date`, `city`).

### Bước 1: Khởi tạo Database và Table trong Hive

```sql
CREATE DATABASE IF NOT EXISTS retail_warehouse;
USE retail_warehouse;

CREATE TABLE IF NOT EXISTS sales_records (
    order_id INT,
    customer_name STRING,
    product STRING,
    category STRING,
    quantity INT,
    price DOUBLE,
    order_date STRING,
    city STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
tblproperties('skip.header.line.count'='1');
```

### Bước 2: Nạp dữ liệu vào bảng (Bulk Loading)

```sql
LOAD DATA LOCAL INPATH '/home/hadoop/shared_data/sales_data.csv' OVERWRITE INTO TABLE sales_records;
```

*Thời gian thực thi: 2.839 giây (Dữ liệu được nạp trực tiếp vào HDFS tại thư mục `/user/hive/warehouse/retail_warehouse.db/sales_records`).*

---

## 3.2. Thực thi câu truy vấn phân tích (Hive MapReduce trên YARN)

### Câu truy vấn HiveQL:

```sql
SELECT 
    category, 
    COUNT(*) AS total_orders, 
    SUM(quantity * price) AS total_revenue, 
    ROUND(AVG(price), 2) AS avg_unit_price
FROM sales_records
GROUP BY category;
```

### Quá trình phân tích thực thi của cụm:

1. **Biên dịch & Tối ưu hóa**: Hive phân tích câu lệnh `GROUP BY category`, dịch thành 1 Job MapReduce phân tán trên YARN (`job_1792292895907_0005`).
2. **Map Phase**: YARN cấp phát Mapper container trên worker node. Mapper đọc từng dòng CSV, trích xuất `key = category` và `value = (quantity * price, price, 1)`.
3. **Shuffle & Sort Phase**: Dữ liệu từ các Mapper được băm (hash-partition) và truyền qua mạng, gom toàn bộ bản ghi có cùng `category` về đúng một Reducer.
4. **Reduce Phase**: Reducer tính tổng số đơn, tổng doanh thu và trung bình đơn giá cho từng nhóm.
5. **Ghi kết quả**: Kết quả cuối cùng được xuất ra màn hình và lưu tạm trên HDFS.

### Kết quả truy vấn thực tế:

```text
MapReduce Jobs Launched: 
Stage-Stage-1: Map: 1  Reduce: 1   Cumulative CPU: 65.87 sec   HDFS Read: 20239 HDFS Write: 165 SUCCESS
Total MapReduce CPU Time Spent: 1 minutes 5 seconds 870 msec

OK
Electronics    10    6305.0    433.0
Furniture      10    4775.0    398.0
Time taken: 92.243 seconds, Fetched: 2 row(s)
```

---

# PHẦN 4: 4 THUẬT TOÁN MAPREDUCE BẰNG PYSPARK TRÊN HADOOP CLUSTER

Tất cả các bài toán đều đọc dữ liệu từ HDFS: `hdfs://toan-master:9000/data_input/input.txt`.

---

## 4.1. Bài toán 1: WordCount (Đếm tần suất từ)

- **Thành viên phụ trách**: **Lê Tuấn Anh** (`anh-slave1`)
- **Mã nguồn**: `pyspark_jobs/01_workcount.py`
- **Nguyên lý giải thuật**:
  - `flatMap`: Tách từng dòng văn bản thành danh sách các từ đơn lẻ.
  - `map`: Chuẩn hóa chữ thường, lọc bỏ ký tự đặc biệt, ánh xạ thành `(word, 1)`.
  - `reduceByKey`: Gom nhóm theo từ và cộng dồn số lần xuất hiện: $\text{count} = \sum 1$.
  - `sortBy`: Sắp xếp giảm dần theo tần suất để lấy Top 20 từ xuất hiện nhiều nhất.
- **Lệnh submit**:
  ```bash
  spark-submit --master yarn --deploy-mode client /home/hadoop/pyspark_jobs/01_workcount.py
  ```

---

## 4.2. Bài toán 2: WordMean (Độ dài trung bình của từ)

- **Thành viên phụ trách**: **Phạm Quốc Huy** (`huy-slave2`)
- **Mã nguồn**: `pyspark_jobs/02_wordmean.py`
- **Công thức toán học**:
  $$
  \bar{L} = \frac{\sum_{i=1}^N \text{len}(w_i)}{N}
  $$

  - $N$: Tổng số lượng từ trong văn bản.
  - $\text{len}(w_i)$: Độ dài (số ký tự) của từ thứ $i$.
- **Nguyên lý giải thuật**:
  - `map`: Ánh xạ mỗi từ thành cặp giá trị: `(len(word), 1)`.
  - `reduce`: Cộng dồn đồng thời cả tổng độ dài và tổng số từ:
    `lambda a, b: (a[0] + b[0], a[1] + b[1])`
  - Kết quả: `Mean = total_length / total_words`.
- **Lệnh submit**:
  ```bash
  spark-submit --master yarn --deploy-mode client /home/hadoop/pyspark_jobs/02_wordmean.py
  ```

---

## 4.3. Bài toán 3: WordMedian (Trung vị độ dài của từ)

- **Thành viên phụ trách**: **Phạm Quốc Huy** (`huy-slave2`)
- **Mã nguồn**: `pyspark_jobs/03_wordmedian.py`
- **Công thức toán học**:
  Sắp xếp dãy độ dài từ theo thứ tự tăng dần: $L_1 \le L_2 \le \dots \le L_N$.
  - Nếu $N$ lẻ: $\text{Median} = L_{(N - 1) / 2}$ (giá trị phần tử tại vị trí trung tâm).
  - Nếu $N$ chẵn: $\text{Median} = \frac{L_{N/2 - 1} + L_{N/2}}{2.0}$.
- **Nguyên lý giải thuật**:
  - `map`: Trích xuất danh sách độ dài các từ thành RDD số nguyên.
  - `sortBy`: Sắp xếp RDD tăng dần.
  - `zipWithIndex`: Đánh chỉ số vị trí cho từng phần tử từ $0$ đến $N-1$.
  - `lookup` / `filter`: Trích xuất phần tử ở vị trí chính giữa để tính giá trị Median.
- **Lệnh submit**:
  ```bash
  spark-submit --master yarn --deploy-mode client /home/hadoop/pyspark_jobs/03_wordmedian.py
  ```

---

## 4.4. Bài toán 4: WordStandardDeviation (Độ lệch chuẩn độ dài từ)

- **Thành viên phụ trách**: **Phạm Đức Khoa** (`khoa-slave3`)
- **Mã nguồn**: `pyspark_jobs/04_workstd.py`
- **Công thức toán học**:

  $$
  \sigma = \sqrt{\frac{1}{N} \sum_{i=1}^N (L_i - \bar{L})^2}
  $$

  - $\bar{L}$: Độ dài trung bình (Mean) đã tính ở Bài toán 2.
  - $L_i$: Độ dài của từ thứ $i$.
  - $N$: Tổng số từ.
- **Nguyên lý giải thuật (Two-pass RDD MapReduce)**:

  - **Pass 1**: Tính $\bar{L}$ và $N$ bằng Reduce.
  - **Pass 2**: Map mỗi từ thành bình phương sai số: $(L_i - \bar{L})^2$.
  - `reduce`: Tính tổng các bình phương sai số: $S = \sum (L_i - \bar{L})^2$.
  - Tính Phương sai (Variance): $\text{Var} = S / N$.
  - Tính Độ lệch chuẩn: $\sigma = \sqrt{\text{Var}}$.
- **Lệnh submit**:

  ```bash
  spark-submit --master yarn --deploy-mode client \
    --num-executors 1 --executor-memory 512m --executor-cores 1 \
    --driver-memory 512m /home/hadoop/pyspark_jobs/04_workstd.py
  ```

### Kết quả thực thi và đối chiếu

Với file `data/input.txt`, chương trình xử lý 423 từ trên 2 phân vùng và cho kết quả:

```text
Độ dài trung bình: 5.791962
Phương sai tổng thể: 8.623387
Độ lệch chuẩn tổng thể: 2.936560
```

Kết quả trên được đối chiếu bằng hàm `statistics.pstdev` của Python và cùng trả về
`2.936560`. Ứng dụng Spark trên YARN kết thúc với trạng thái `FINISHED/SUCCEEDED`;
executor và các task được YARN cấp chạy trên `khoa-slave3`.

### Đo thời gian

| Chế độ                  |   Số executor   | Thời gian xử lý RDD | Tổng thời gian lệnh |
| :------------------------- | :---------------: | ---------------------: | ---------------------: |
| Spark local (`local[2]`) | 1 driver cục bộ |            3.745 giây |           12.353 giây |
| Spark trên YARN           |    1 executor    |            4.361 giây |           28.831 giây |

Tập dữ liệu kiểm thử chỉ khoảng 2.9 KB nên thời gian khởi tạo ứng dụng, tải thư viện
Spark lên HDFS và xin tài nguyên từ YARN lớn hơn thời gian tính toán. Vì vậy kết quả này
dùng để xác nhận chương trình chạy phân tán đúng, chưa đủ để kết luận Spark local nhanh
hơn Spark trên YARN với dữ liệu lớn.

---

## 4.5. Bài toán 5 (Nâng cao): E-commerce Retail Analytics với Spark SQL

- **Thành viên phụ trách**: **Nguyễn Đức Toàn** (`toan-master`)
- **Mã nguồn**: `pyspark_jobs/05_ecommerce_analytics.py`
- **Mô tả**:
  - Đọc file `sales_data.csv` bằng PySpark DataFrame API.
  - Đăng ký bảng tạm (`createOrReplaceTempView`).
  - Thực hiện các phân tích đa chiều: Top doanh thu theo danh mục, Top 3 sản phẩm bán chạy, Phân bổ doanh thu theo thành phố.
  - So sánh hiệu năng: Spark SQL xử lý trong bộ nhớ (In-memory) hoàn tất trong vài giây, vượt trội hơn so với MapReduce cổ điển của Hive.

---

# PHẦN 5: BỘ CÂU HỎI VẤN ĐÁP BẢO VỆ ĐỒ ÁN (DÀNH CHO NHÓM ÔN TẬP)

### Câu 1: Tại sao cụm lại cần SecondaryNameNode? SecondaryNameNode có thay thế được NameNode khi NameNode chết không?

- **Trả lời**: SecondaryNameNode **KHÔNG PHẢI** là máy chủ dự phòng (Standby NameNode). Nhiệm vụ của nó là định kỳ tải file `fsimage` (ảnh chụp hệ thống tệp) và file `edits` (nhật ký thao tác) từ NameNode về để hợp nhất (checkpoint), tạo ra file `fsimage.ckpt` mới rồi gửi ngược lại cho NameNode. Điều này giúp file nhật ký `edits` của NameNode không bị phình to vô hạn, giúp NameNode khởi động nhanh hơn.

### Câu 2: Làm sao chứng minh dữ liệu và tính toán được phân tán trên 3 máy Worker mà không phải chạy cục bộ trên 1 máy?

- **Trả lời**:
  1. Dùng lệnh `hdfs fsck /user/hive/warehouse/retail_warehouse.db/sales_records -files -blocks -locations`, kết quả in ra rõ ràng các block dữ liệu được sao chép sang cả 3 máy: `172.28.0.3` (`anh-slave1`), `172.28.0.4` (`huy-slave2`), `172.28.0.5` (`khoa-slave3`).
  2. Truy cập YARN Web UI ([http://localhost:8088](http://localhost:8088)) và JobHistory UI ([http://localhost:19888](http://localhost:19888)): Kiểm tra danh sách Task Containers sẽ thấy Mapper và Reducer được cấp phát trên các node slave khác nhau dựa trên cơ chế Data Locality.

### Câu 3: Sự khác biệt cơ bản giữa Apache Hive và Apache Spark là gì?

- **Trả lời**:
  - **Hive**: Là Data Warehouse lưu trữ trên HDFS, chuyển đổi SQL thành các job MapReduce cổ điển. Xử lý ghi/đọc liên tục xuống đĩa cứng (Disk I/O) nên độ trễ cao, phù hợp cho các bài toán phân tích hàng loạt (Batch Processing) ban đêm hoặc khối dữ liệu khổng lồ vài chục Terabyte.
  - **Spark**: Là engine tính toán trong bộ nhớ (In-Memory Processing). Dữ liệu được lưu trong RAM dưới dạng các RDD / DataFrame, xử lý theo đồ thị có hướng (DAG), giúp tốc độ nhanh hơn Hive từ 10 đến 100 lần.

### Câu 4: Lỗi `ClassNotFoundException: org.apache.hadoop.mapreduce.v2.app.MRAppMaster` đã từng xảy ra trên cụm và nhóm đã khắc phục như thế nào?

- **Trả lời**: Trên Hadoop 3.x, khi chạy MapReduce trên YARN, ApplicationMaster cần biết chính xác thư mục chứa các thư viện mapreduce. Nhóm đã bổ sung biến môi trường `yarn.app.mapreduce.am.env=HADOOP_MAPRED_HOME=/opt/hadoop` trong `mapred-site.xml` và khai báo biến `yarn.application.classpath` trong `yarn-site.xml` để YARN nạp đầy đủ classpath cho container.
