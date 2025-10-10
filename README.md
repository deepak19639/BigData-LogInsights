# 🚀 BigData-LogInsights (SparkLogParser)

## 📘 Overview
This project demonstrates how to parse and analyze massive web server logs using **Apache Spark (PySpark)**.  
It focuses on building **data processing and analysis pipelines** that extract meaningful insights from unstructured log data.

---

## 📁 Project Structure
```
BigData-LogInsights/
├── NASA_access_log_Jul95.gz
├── spark_log_parser.py
└── README.md
```

---

## 🧩 Steps Performed

### 1️⃣ See the Structure of Your Data
```python
df.printSchema()
df.show(5, truncate=False)
```
**Example Output:**
```
host	timestamp	request	status	bytes
199.72.81.55	01/Jul/1995:00:00:01 -0400	GET /history/apollo/ HTTP/1.0	200	6245
```

---

### 💥 2️⃣ Total Requests
```python
df.count()
```
➡️ Gives the total number of log entries (i.e., requests).

---

### ⚙️ 3️⃣ Top 10 Frequent Hosts (who accessed most)
```python
df.groupBy("host").count().orderBy("count", ascending=False).show(10)
```
➡️ Shows which IPs or domains made the most requests.

---

### 📈 4️⃣ Top Requested URLs (Most Popular Pages)
```python
from pyspark.sql.functions import regexp_extract

# Extract the URL from the request field
df = df.withColumn("endpoint", regexp_extract("request", r'^\S+ (\S+)', 1))

df.groupBy("endpoint").count().orderBy("count", ascending=False).show(10, truncate=False)
```
➡️ Shows the most requested pages on the website.

---

### 🚫 5️⃣ Error Analysis (HTTP Status Codes)
```python
df.groupBy("status").count().orderBy("count", ascending=False).show()
```
➡️ Find which status codes (200, 404, 500, etc.) appear most often.

Filter only failed requests:
```python
df.filter(df.status >= 400).groupBy("status").count().orderBy("count", ascending=False).show()
```

---

### ⏰ 6️⃣ Requests Over Time (Hourly or Daily Trends)
You can parse timestamp to extract hour or date.
```python
from pyspark.sql.functions import to_timestamp, date_format

df = df.withColumn("datetime", to_timestamp(df.timestamp, "dd/MMM/yyyy:HH:mm:ss Z"))
df = df.withColumn("hour", date_format("datetime", "HH"))
df = df.withColumn("date", date_format("datetime", "yyyy-MM-dd"))

df.groupBy("date").count().orderBy("date").show(10)
```
➡️ Shows number of requests per day. (You can also group by hour for hourly traffic trends.)

---

### 📦 7️⃣ Total Bytes Transferred
```python
from pyspark.sql.functions import sum as _sum

df.select(_sum("bytes").alias("total_bytes")).show()
```
➡️ Shows total data (in bytes) sent to all clients.

---

### 🌍 8️⃣ Detect Bots / Crawlers
```python
df.groupBy("host").count().filter("count > 1000").orderBy("count", ascending=False).show(10)
```
➡️ Likely automated crawlers or bots.

---

### 🔥 9️⃣ Combine Insights — Top Pages with Most Errors
```python
df.filter(df.status >= 400).groupBy("endpoint").count().orderBy("count", ascending=False).show(10, truncate=False)
```
➡️ Which pages returned the most errors.

---

### 📊 10️⃣ Visualize (Optional in Colab)
You can collect small data and use matplotlib:
```python
import matplotlib.pyplot as plt

top_pages = df.groupBy("endpoint").count().orderBy("count", ascending=False).limit(10).toPandas()
plt.barh(top_pages['endpoint'], top_pages['count'])
plt.xlabel("Hits")
plt.ylabel("Pages")
plt.title("Top 10 Most Requested Pages")
plt.show()
```

---

## ⚙️ Tools & Technologies
- Apache Spark (PySpark)
- Google Colab
- Python (Regex, DataFrames)
- NASA HTTP Web Server Logs (July 1995)

---

## 🎯 Learning Outcomes
✔️ Parse unstructured log data with Regex  
✔️ Build RDD → DataFrame pipelines  
✔️ Perform transformations and aggregations in PySpark  
✔️ Generate insights from log data  
✔️ Create a clean, professional GitHub project

---

## 👨‍💻 Author
**Deepak Kumar**  
Data Engineer | Passionate about Big Data & Automation  
[GitHub Profile](https://github.com/)
