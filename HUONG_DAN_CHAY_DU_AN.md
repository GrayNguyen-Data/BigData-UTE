# HƯỚNG DẪN THỰC THI DỰ ÁN CHO THÀNH VIÊN NHÓM
## Đề tài: Cụm Big Data Phân Tán (Hadoop, YARN, Hive, Spark) trên Docker Multi-Node

Tài liệu này hướng dẫn chi tiết từng bước từ A đến Z dành cho các thành viên trong nhóm (**Nguyễn Đức Toàn**, **Lê Tuấn Anh**, **Phạm Quốc Huy**, **Trần Đăng Khoa**) để khởi động cụm, viết mã nguồn bài toán được phân công, nộp job lên YARN và chụp ảnh minh chứng làm báo cáo.

---

## ⚙️ 1. YÊU CẦU BAN ĐẦU
1. Máy tính đã cài đặt **Docker Desktop** (và Docker Desktop đang ở trạng thái **Running**).
2. Đã tải hoặc clone thư mục dự án `Bigdata-project` về máy tính.
3. Mở phần mềm dòng lệnh (**PowerShell** trên Windows hoặc **Terminal** trên macOS/Linux) tại thư mục `Bigdata-project`.

---

## 🚀 2. QUY TRÌNH THỰC THI TỪNG BƯỚC

### BƯỚC 1: Khởi động Cụm 5 Container Docker
Mở PowerShell tại thư mục dự án `Bigdata-project`, chạy lệnh:
```powershell
docker compose up -d
```
Kiểm tra trạng thái các container:
```powershell
docker compose ps
```
> **Yêu cầu đạt được**: Thấy đủ 5 container (`toan-master`, `anh-slave1`, `huy-slave2`, `khoa-slave3`, `postgres-metastore`) đều hiển thị trạng thái **Up**.

---

### BƯỚC 2: Đăng nhập vào Master Node (`toan-master`)
Chạy lệnh:
```powershell
docker exec -it toan-master bash
```
> Lúc này con trỏ terminal sẽ chuyển thành: `hadoop@toan-master:~$` (nghĩa là bạn đã ở bên trong máy chủ Master).

---

### BƯỚC 3: Khởi động các Dịch vụ và Nạp Dữ liệu lên HDFS
Trong terminal `hadoop@toan-master:~$`, chạy lần lượt các script tự động:

```bash
# 1. Khởi động Hadoop Distributed File System (HDFS), YARN và JobHistoryServer:
bash /home/hadoop/scripts/start-hadoop.sh

# 2. Đẩy các file dữ liệu mẫu (input.txt và sales_data.csv) lên HDFS /data_input:
bash /home/hadoop/scripts/upload-data-to-hdfs.sh

# 3. (Tùy chọn) Khởi động Apache Hive Metastore & HiveServer2:
bash /home/hadoop/scripts/start-hive.sh
```

**Kiểm tra tiến trình Java (JPS)**:
Gõ lệnh:
```bash
jps
```
Trên `toan-master` cần hiển thị đủ các dịch vụ:
- `NameNode`
- `SecondaryNameNode`
- `ResourceManager`
- `JobHistoryServer`
- `RunJar` (Hive Metastore)
- `RunJar` (HiveServer2)

---

### BƯỚC 4: Viết Mã Nguồn Thuật Toán Được Phân Công
Các bạn mở VS Code trên máy thật của mình và mở file tương ứng trong thư mục `pyspark_jobs/`:

| Thành viên | File cần chỉnh sửa | Bài toán đảm nhiệm |
|:---|:---|:---|
| **Lê Tuấn Anh** | `pyspark_jobs/01_workcount.py` | **WordCount**: Đếm tần suất xuất hiện của từ |
| **Phạm Quốc Huy** | `pyspark_jobs/02_wordmean.py` | **WordMean**: Tính độ dài trung bình của từ |
| **Phạm Quốc Huy** | `pyspark_jobs/03_wordmedian.py` | **WordMedian**: Tính trung vị độ dài của từ |
| **Trần Đăng Khoa** | `pyspark_jobs/04_workstd.py` | **WordStd**: Tính độ lệch chuẩn độ dài từ |
| **Nguyễn Đức Toàn** | `pyspark_jobs/05_ecommerce_analytics.py` | **Retail Analytics**: Phân tích dữ liệu bán hàng Spark SQL |

> **Lưu ý cực kỳ tiện lợi**: Thư mục `pyspark_jobs/` trên máy thật được gắn trực tiếp (Volume Mount) vào `/home/hadoop/pyspark_jobs/` bên trong container. Do đó, bạn chỉ cần sửa code và bấm Save (Ctrl + S) trên máy thật, trong container sẽ được cập nhật ngay lập tức mà **không cần copy lại**.

---

### BƯỚC 5: Nộp Job PySpark lên Cụm YARN Cluster
Quay lại terminal `hadoop@toan-master:~$` và gõ lệnh chạy tương ứng bài của mình:

#### 1. Bài của bạn Lê Tuấn Anh:
```bash
spark-submit --master yarn /home/hadoop/pyspark_jobs/01_workcount.py
```

#### 2. Bài của bạn Phạm Quốc Huy:
```bash
spark-submit --master yarn /home/hadoop/pyspark_jobs/02_wordmean.py
spark-submit --master yarn /home/hadoop/pyspark_jobs/03_wordmedian.py
```

#### 3. Bài của bạn Trần Đăng Khoa:
```bash
spark-submit --master yarn /home/hadoop/pyspark_jobs/04_workstd.py
```

#### 4. Bài của bạn Nguyễn Đức Toàn:
```bash
spark-submit --master yarn /home/hadoop/pyspark_jobs/05_ecommerce_analytics.py
```

---

### BƯỚC 6: Demo Apache Hive Data Warehouse (Dành cho phần Warehouse)
Nếu cần chụp ảnh demo Data Warehouse của Apache Hive:
```bash
# 1. Mở Hive CLI trên toan-master:
hive
```
Trong dấu nhắc `hive>`, copy và dán đoạn lệnh SQL này:
```sql
USE retail_warehouse;

-- Tạo bảng sales_records
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

-- Nạp toàn bộ dữ liệu CSV vào bảng Data Warehouse:
LOAD DATA LOCAL INPATH '/home/hadoop/shared_data/sales_data.csv' OVERWRITE INTO TABLE sales_records;

-- Chạy câu truy vấn tổng hợp phân tích doanh thu bằng MapReduce trên cụm:
SELECT 
    category, 
    COUNT(*) AS total_orders, 
    SUM(quantity * price) AS total_revenue, 
    ROUND(AVG(price), 2) AS avg_unit_price
FROM sales_records
GROUP BY category;
```
*(Chờ MapReduce chạy xong, gõ `exit;` để thoát Hive).*

---

## 📸 3. DANH MỤC ẢNH CẦN CHỤP ĐỂ DÁN VÀO BÁO CÁO WORD

Mỗi bạn chạy xong cần chụp các ảnh sau:

1. **Ảnh Terminal chạy code của mình**:
   - Chụp rõ dòng lệnh `spark-submit --master yarn ...`
   - Chụp bảng kết quả tính toán in ra màn hình.
2. **Ảnh YARN Web UI**:
   - Mở trình duyệt vào địa chỉ: [http://localhost:8088](http://localhost:8088)
   - Chụp dòng Application của bài toán mình vừa chạy với trạng thái: `State: FINISHED`, `FinalStatus: SUCCEEDED`.
3. **Ảnh HDFS Web UI**:
   - Mở trình duyệt vào: [http://localhost:9870](http://localhost:9870) (thấy 3 DataNode Live).
   - Mở File Browser: [http://localhost:9870/explorer.html#/data_input](http://localhost:9870/explorer.html#/data_input) (thấy các file `input.txt` và `sales_data.csv`).
4. **Ảnh MapReduce JobHistory UI**:
   - Mở trình duyệt vào: [http://localhost:19888](http://localhost:19888) (xem chi tiết số lượng Mapper/Reducer).

---

## 🛑 4. CÁC LỆNH TIỆN ÍCH KHI QUẢN TRỊ CỤM

- **Thoát khỏi container Master về lại máy thật**: Gõ `exit` trong terminal container.
- **Tắt toàn bộ cụm khi học xong (để giải phóng RAM & pin laptop)**:
  ```powershell
  docker compose down
  ```
- **Khởi động lại cụm vào hôm sau**:
  ```powershell
  docker compose up -d
  docker exec -it toan-master bash
  bash /home/hadoop/scripts/start-hadoop.sh
  ```
