# Khung báo cáo và ôn tập bảo vệ

Tài liệu này đề xuất cấu trúc báo cáo dựa trên báo cáo mẫu và yêu cầu môn học, đồng thời ghi lại các điểm cần xác minh trong repository trước khi chốt nội dung. Đây là khung làm việc, không phải xác nhận rằng các job đã chạy thành công.

## 1. Tình trạng rà soát hiện tại

- Năm file trong `pyspark_jobs/` hiện đều rỗng (0 byte). Vì vậy chưa thể xác nhận thuật toán, kết quả đầu ra hay lệnh `spark-submit` chạy thành công từ source hiện có.
- `docker compose config --quiet` hợp lệ. Tuy nhiên máy hiện không kết nối được Docker Desktop Engine nên chưa thể build image, khởi động container hoặc kiểm chứng runtime.
- `docker-compose.yml` tham chiếu image `bigdata-cluster:3.3.6` nhưng không có phần `build:`. Hướng dẫn hiện tại chưa nêu lệnh build image; cần tạo image này trước khi `docker compose up -d` nếu máy chưa có image.
- Các con số/thời gian/job ID và trạng thái hoàn thành trong bản thảo báo cáo là nội dung chưa được kiểm chứng trong lần rà soát này. Chỉ giữ lại khi nhóm có log hoặc ảnh chụp tương ứng.
- Dự án dùng Spark trên YARN. Các phép toán RDD có thể trình bày là cách xử lý **theo mô hình MapReduce**, nhưng không nên gọi đó là Hadoop MapReduce job. MapReduce JobHistory UI cổng 19888 dành cho Hadoop MapReduce; Spark application nên kiểm chứng trên YARN UI.
- `spark-defaults.conf` khai báo địa chỉ Spark History Server tại cổng 18080 nhưng repository chưa có bước khởi chạy server này và Compose chưa publish cổng 18080.
- Dữ liệu mẫu nhỏ (20 giao dịch CSV và văn bản vài KB); phù hợp minh họa chức năng, không đủ để khẳng định hiệu năng Big Data hay scalability.
- Compose cảnh báo trường `version` đã cũ nhưng cấu hình vẫn hợp lệ. Hiện chưa thể đánh giá các dependency tải trong Dockerfile vì chưa chạy build.

## 2. Cấu trúc báo cáo đề xuất

### Trang bìa và phần đầu

1. Trang bìa theo mẫu của khoa: trường/khoa, tên môn, tên đề tài, giảng viên, lớp học phần, nhóm và thành viên.
2. Bảng phân công công việc. Ghi đúng phần mỗi thành viên thực sự đã làm; yêu cầu môn học nêu mọi thành viên đều tham gia cài đặt, viết và thực thi chương trình.
3. Lời cảm ơn (nếu cần), mục lục, danh mục hình/bảng và danh mục từ viết tắt.

### Chương 1 — Giới thiệu đề tài

- Bối cảnh bài toán dữ liệu lớn và mục tiêu của nhóm.
- Phạm vi: Hadoop HDFS/YARN, Hive Warehouse, Spark/PySpark trên YARN và bốn bài toán văn bản.
- Môi trường thực hiện: Docker nhiều container trên một máy hay trên nhiều máy; nêu rõ lựa chọn thực tế, không gọi các container là các laptop vật lý độc lập.
- Tiêu chí kiểm chứng: dịch vụ hoạt động, dữ liệu có trên HDFS, job được YARN tiếp nhận, kết quả khớp phép tính kiểm tra.

### Chương 2 — Cơ sở lý thuyết

- Hadoop: vai trò HDFS, YARN và Hadoop MapReduce.
- HDFS: NameNode/DataNode, block, replication và giới hạn của SecondaryNameNode (checkpoint, không phải standby NameNode).
- YARN: ResourceManager, NodeManager, ApplicationMaster và container.
- MapReduce: Map → Shuffle/Sort → Reduce.
- Hive: HiveServer2, Metastore, PostgreSQL lưu metadata; dữ liệu bảng đặt trên HDFS.
- Spark/PySpark: driver, executor, RDD/DataFrame, lazy transformations và actions.
- Phân biệt rõ Hadoop MapReduce với Spark chạy trên YARN; tránh khẳng định mọi truy vấn Hive/Spark luôn chạy bằng cùng một engine.

### Chương 3 — Thiết kế và cài đặt cụm

- Sơ đồ 5 container: `toan-master`, ba worker và `postgres-metastore`; mô tả các địa chỉ IP/port theo `docker-compose.yml`.
- Bảng phiên bản dự kiến theo Dockerfile: Ubuntu 22.04, Hadoop 3.3.6, Spark 3.5.8, Hive 3.1.3 và Java 8. Chỉ ghi “đã cài/đã chạy” sau khi xác minh build và runtime.
- Vai trò từng file cấu hình: `core-site.xml`, `hdfs-site.xml`, `yarn-site.xml`, `mapred-site.xml`, `hive-site.xml`, `spark-defaults.conf`, `workers`.
- Trình tự dựng image, khởi động container, khởi động Hadoop, nạp dữ liệu HDFS và khởi động Hive.
- Ảnh minh chứng: `docker compose ps`, `jps`, HDFS UI, YARN UI, HDFS file listing, Hive query.

### Chương 4 — Hive Data Warehouse

- Mô tả lược đồ `sales_data.csv` và bảng `sales_records`.
- Trình bày cách tạo database/table và nạp dữ liệu.
- Giải thích truy vấn theo `category`: `COUNT(*)`, `SUM(quantity * price)`, `AVG(price)`.
- Đưa kết quả thật, lệnh thực thi và bằng chứng engine/job từ log. Không dùng lại job ID/thời gian mẫu nếu không khớp lần chạy của nhóm.

### Chương 5 — Các bài toán xử lý văn bản bằng PySpark

Mỗi bài có cùng bố cục: mục tiêu → định nghĩa dữ liệu đầu vào/token → công thức → các bước xử lý → lệnh chạy → kết quả thật → đối chiếu thủ công hoặc bằng chương trình tuần tự.

1. WordCount: đếm số lần xuất hiện của từng từ.
2. WordMean: tổng độ dài từ chia tổng số từ.
3. WordMedian: sắp xếp độ dài; chọn phần tử giữa hoặc trung bình hai phần tử giữa.
4. WordStandardDeviation: nêu rõ dùng độ lệch chuẩn tổng thể (chia cho `N`) hay mẫu (chia cho `N-1`); công thức trong bản thảo hiện là tổng thể.

Mỗi chương trình cần nêu cách chuẩn hóa chữ hoa/thường, dấu câu, từ rỗng và quy ước độ dài ký tự. Hiện source chưa có nội dung nên chưa thể xác nhận các quyết định này.

### Chương 6 — Kết quả, kiểm thử và thảo luận

- Bảng cho từng job: lệnh, trạng thái YARN, thời gian thực đo, kết quả chính, nguồn minh chứng.
- Phân biệt output của driver, YARN application và Hadoop MapReduce JobHistory.
- Kiểm thử kết quả với tập dữ liệu nhỏ có thể tính tay; ghi rõ số lượng từ/bản ghi.
- Nêu giới hạn: dữ liệu demo nhỏ, Docker nhiều container có thể cùng chạy trên một host, chưa có benchmark quy mô lớn.
- Phân tích lỗi đã gặp và cách xử lý chỉ khi có log/bằng chứng.

### Chương 7 — Kết luận và hướng phát triển

- Tóm tắt mục tiêu nào đã được kiểm chứng và mục tiêu nào còn dang dở.
- Hướng phát triển: bổ sung dữ liệu lớn hơn, tự động hóa kiểm thử, health check cho dịch vụ, lưu script SQL và khởi chạy Spark History Server.

### Tài liệu tham khảo và phụ lục

- Tài liệu Apache Hadoop, Spark, Hive; Docker Compose; tài liệu/hướng dẫn môn học.
- Phụ lục: lệnh cài đặt/chạy, cấu hình quan trọng, mã nguồn, log và ảnh chụp minh chứng.

## 3. MapReduce thực thi như thế nào?

### Hadoop MapReduce cổ điển

1. File trên HDFS được chia thành input split; mỗi Mapper xử lý một split. HDFS block là đơn vị lưu trữ, còn input split là đơn vị logic cho Map task và không phải lúc nào cũng trùng tuyệt đối với block.
2. **Map** đọc record và phát sinh các cặp trung gian `(key, value)`. Ví dụ WordCount: từng từ → `(từ, 1)`.
3. (Tùy chọn) **Combiner** gộp cục bộ kết quả trên Mapper để giảm dữ liệu truyền qua mạng; chỉ dùng khi phép gộp phù hợp. Không nên nói dự án có Combiner nếu source không cài đặt nó.
4. **Partitioner + Shuffle/Sort** phân phối cùng key về cùng Reducer, truyền dữ liệu qua mạng và sắp xếp/nhóm các key.
5. **Reduce** tổng hợp values theo key; ví dụ WordCount cộng các số `1`.
6. Kết quả được ghi ra HDFS. YARN cấp tài nguyên cho ApplicationMaster và các task container; NodeManager trên worker khởi chạy/giám sát container.

### Cùng ý tưởng trên Spark/YARN của dự án

- `spark-submit --master yarn --deploy-mode client` nộp một Spark application lên YARN; với `client`, driver ở tiến trình submit trên master container, còn executors được YARN cấp phát trong các container.
- Các transformation như `flatMap`, `map`, `reduceByKey` được đánh giá lười; action như `count`, `collect`, `reduce` mới kích hoạt job.
- Spark lập DAG, chia thành stages/tasks; thao tác shuffle (ví dụ `reduceByKey`) tạo ranh giới stage và trao đổi dữ liệu giữa partitions.
- Đây là **xử lý kiểu MapReduce bằng Spark**, không phải job Hadoop MapReduce. Không mặc định thấy Spark job trong giao diện JobHistory Hadoop ở cổng 19888.
- Để chứng minh phân tán, chụp YARN application có trạng thái thành công và thông tin executor/container/node; với HDFS, kiểm tra `hdfs dfsadmin -report` hoặc `hdfs fsck`. File demo nhỏ có thể chỉ tạo một partition/task nên không chứng minh tải được chia rộng trên các worker.

## 4. Quy trình setup và kiểm chứng

Chạy PowerShell tại thư mục dự án. Trước hết mở Docker Desktop và chờ Linux Engine hoạt động.

```powershell
docker build -t bigdata-cluster:3.3.6 .
docker compose up -d
docker compose ps
docker exec -it toan-master bash
```

Lệnh `docker build` cần thiết vì Compose hiện tham chiếu image nhưng không khai báo `build:`. Nếu image đã có sẵn đúng phiên bản, bước build có thể không cần; cần xác nhận bằng `docker image ls`.

Trong terminal của `toan-master`:

```bash
bash /home/hadoop/scripts/start-hadoop.sh
hdfs dfsadmin -report
yarn node -list
bash /home/hadoop/scripts/upload-data-to-hdfs.sh
hdfs dfs -ls -h /data_input
```

Khởi động Hive sau khi xác nhận PostgreSQL đã sẵn sàng:

```bash
bash /home/hadoop/scripts/start-hive.sh
hive
```

Trong Hive CLI, tạo database/bảng theo schema trong báo cáo, sau đó chạy truy vấn kiểm tra. Với lần chạy tiếp theo, không format lại NameNode nếu volume HDFS đã chứa dữ liệu.

Chỉ chạy các lệnh sau sau khi source tương ứng đã được viết và kiểm tra:

```bash
spark-submit --master yarn --deploy-mode client /home/hadoop/pyspark_jobs/01_workcount.py
spark-submit --master yarn --deploy-mode client /home/hadoop/pyspark_jobs/02_wordmean.py
spark-submit --master yarn --deploy-mode client /home/hadoop/pyspark_jobs/03_workmedian.py
spark-submit --master yarn --deploy-mode client /home/hadoop/pyspark_jobs/04_workstd.py
spark-submit --master yarn --deploy-mode client /home/hadoop/pyspark_jobs/05_ecommerce_analytics.py
```

### Giao diện nên dùng làm minh chứng

| Nội dung | Nơi kiểm tra |
|---|---|
| HDFS NameNode, DataNode sống, file HDFS | `http://localhost:9870` |
| Spark application được YARN tiếp nhận | `http://localhost:8088` |
| Hadoop MapReduce job (chỉ khi thực sự chạy Hadoop MapReduce) | `http://localhost:19888` |
| Spark UI | Cổng `4040` khi ứng dụng đang chạy; cần xác nhận port mapping và driver UI |
| Spark History UI | Chưa được cấu hình đầy đủ để khẳng định hoạt động; cần triển khai History Server và kiểm tra event log trước |

## 5. Câu hỏi bảo vệ có thể gặp

1. **HDFS, YARN và MapReduce khác nhau thế nào?**  
   HDFS lưu dữ liệu phân tán; YARN quản lý tài nguyên và container; MapReduce là mô hình/framework xử lý Map–Shuffle–Reduce.

2. **Map, Shuffle/Sort và Reduce làm gì?**  
   Map phát sinh cặp key/value; shuffle phân phối cùng key về một nhóm/reducer; reduce tổng hợp values của từng key.

3. **WordCount hoạt động ra sao?**  
   Tách dòng thành token, chuẩn hóa theo quy ước, phát sinh `(token, 1)`, rồi cộng các giá trị theo token.

4. **Spark `reduceByKey` có đồng nghĩa với Hadoop MapReduce không?**  
   Không. Đây là API Spark thể hiện phép gộp theo key; Spark chạy DAG/stages trên executor do YARN cấp phát, còn Hadoop MapReduce chạy job theo framework MapReduce.

5. **Vì sao dùng `reduceByKey` thay vì `groupByKey` cho phép cộng?**  
   `reduceByKey` có thể gộp trước trên từng partition, giảm lượng dữ liệu shuffle; `groupByKey` cần chuyển toàn bộ values theo key.

6. **YARN tham gia lúc nào khi submit Spark?**  
   YARN cấp tài nguyên và khởi chạy ApplicationMaster/executor containers; Spark driver lập lịch DAG/tasks.

7. **Client deploy mode của dự án có nghĩa gì?**  
   Driver chạy ở tiến trình submit trên master; executors chạy trong YARN containers. Nếu tiến trình submit/host mất, driver client cũng có thể mất.

8. **SecondaryNameNode có thay thế NameNode khi NameNode hỏng không?**  
   Không. Nó định kỳ checkpoint metadata; HA/failover cần kiến trúc khác.

9. **Replication factor bằng 3 chứng minh phép tính được chạy trên 3 worker không?**  
   Không. Nó nói về số bản sao block HDFS; cần bằng chứng về container/executor/task trên các node. Với file nhỏ, một task có thể đủ xử lý.

10. **PostgreSQL lưu dữ liệu bán hàng Hive hay metadata?**  
    Cấu hình hiện tại dùng PostgreSQL làm Hive Metastore (metadata/schema); file bảng được lưu ở warehouse trên HDFS.

11. **Trung vị và độ lệch chuẩn được tính thế nào?**  
    Trung vị cần tìm vị trí giữa sau sắp xếp; độ lệch chuẩn cần nói rõ mẫu hay tổng thể. Công thức đang mô tả là tổng thể, chia cho `N`.

12. **Nếu job thất bại thì kiểm tra gì?**  
    Xem trạng thái/log application trên YARN, log driver/container, cấu hình classpath, HDFS input/output và tài nguyên YARN; không chỉ dựa vào dòng cuối ở terminal.

13. **Quy mô dữ liệu hiện tại có đủ chứng minh Big Data không?**  
    Không. Đây là demo kiểm tra chức năng; cần dataset lớn hơn và benchmark có phương pháp nếu muốn kết luận về hiệu năng/mở rộng.

14. **Vì sao chọn Docker multi-container và giới hạn là gì?**  
    Dễ tái lập các vai trò master/worker trong môi trường học tập; nếu tất cả container cùng một máy thì vẫn chia tiến trình/node logic nhưng không chứng minh chịu lỗi phần cứng giữa nhiều máy.

## 6. Checklist trước khi nộp

- [ ] Năm file PySpark có mã nguồn, chạy được và kết quả khớp kiểm thử độc lập.
- [ ] Docker image build thành công; đủ năm container ở trạng thái hoạt động.
- [ ] HDFS/YARN/Hive được kiểm tra bằng lệnh/UI, không chỉ dựa vào script đã in thông báo.
- [ ] Dữ liệu có trên HDFS và Hive query trả kết quả đúng.
- [ ] YARN application của từng job có trạng thái thành công; lưu log và ảnh chụp.
- [ ] Báo cáo phân biệt Spark-on-YARN với Hadoop MapReduce; không gán Spark job vào Hadoop JobHistory.
- [ ] Mọi thời gian, job ID, số Mapper/Reducer, số liệu và tỷ lệ hoàn thành đều là kết quả thật có bằng chứng.
- [ ] Bảng phân công phản ánh đúng yêu cầu và đóng góp thực tế; rà lại học kỳ/ngày nộp vì tên file hướng dẫn và nội dung trong DOCX đang ghi học kỳ khác nhau.
- [ ] Ghi rõ giới hạn của dữ liệu nhỏ, phạm vi triển khai Docker và những phần chưa kiểm chứng.
