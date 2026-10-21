
import re
from pyspark.sql import SparkSession

# 1. Khoi tao Spark
spark = SparkSession.builder \
    .appName("WordCount-VietAnh") \
    .getOrCreate()

sc = spark.sparkContext

# 2. Doc du lieu tu HDFS
input_path = "hdfs://toan-master:9000/data_input/input.txt"
lines = sc.textFile(input_path, minPartitions=3)

# 3. MAP: Tach tu va chuyen thanh chu thuong
words = lines.flatMap(
    lambda line: re.findall(r"\b\w+\b", line.lower())
)

# 4. MAP: Gan moi tu voi gia tri 1
word_pairs = words.map(lambda word: (word, 1))

# 5. REDUCE: Gom cac tu giong nhau va tinh tong
word_counts = word_pairs.reduceByKey(
    lambda a, b: a + b,
    numPartitions=3
)

# 6. Lay 20 tu xuat hien nhieu nhat
top20 = word_counts.takeOrdered(
    20,
    key=lambda item: (-item[1], item[0])
)

# 7. In ket qua
print("\n========== TOP 20 WORD COUNT ==========")

for word, count in top20:
    print(f"{word}: {count}")

print("=======================================\n")

# 8. Ket thuc Spark
spark.stop()
