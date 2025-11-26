# PROMPT: AWS Glue (Spark) Optimization and Diagnosis

**Persona:** Act as a Principal Data Engineer and AWS FinOps Specialist. You have deep knowledge of Apache Spark's internal architecture (Catalyst Optimizer, Tungsten Memory Management, Shuffle Service) and the AWS Glue infrastructure.

**Objective:** Analyze the technical data, code, and, most importantly, the **LOGS AND METRICS EVIDENCE** provided below to diagnose the root cause of the job's inefficiency or failure.

---

## 1. Job Technical Context

| **Parameter**           | **Value**                                                               |
| ----------------------- | ----------------------------------------------------------------------- |
| **Job Objective**       | `[e.g., Ingest sales data from ERP to Data Lake]`                       |
| **Glue Version**        | `[e.g., Glue 4.0 (Spark 3.3)]`                                          |
| **Worker Type**         | `[e.g., G.1X / G.2X / Z.2X]`                                            |
| **Number of Workers**   | `[e.g., 50 fixed OR Auto Scaling enabled (Min 2, Max 50)]`              |
| **Active Features**     | `[e.g., Job Bookmarks, Glue Flex]`                                      |

### Data Profile (Source)

- **Format:** `[e.g., JSON / Parquet / CSV]`
- **Total Volume:** `[e.g., 500 GB]`
- **File Structure:** `[e.g., Thousands of small files (~100KB each) in S3]`

### Data Profile (Destination)

- **Format:** `[e.g., Parquet Snappy]`
- **Partitioning:** `[e.g., Partitioned by year/month/day]`

---

## 2. Execution Evidence (Where the problem is)

### A. Error Logs (CloudWatch / S3 stderr)

*(Paste the error Stack Trace here. Look for "Caused by", "OutOfMemoryError", "ExecutorLostFailure")*

> ```
> [PASTE THE ERROR HERE]
> ```

### B. Spark UI Analysis (Performance)

*(Data extracted from Spark History Server or S3 Event Logs)*

- **Bottleneck Stage (ID and Duration):** `[e.g., Stage 8 took 1h (90% of total time)]`
- **Stage Operation:** `[e.g., SortMergeJoin / GroupBy]`
- **Shuffle Read/Write:** `[e.g., The stage wrote 200GB of Shuffle]`
- **Data Skew:** `[e.g., The median task takes 2min, but the MAX task takes 45min]`
- **GC Time (Garbage Collection):** `[e.g., Low (<5%) OR High (>15% indicating memory pressure)]`

### C. Infrastructure Metrics (CloudWatch Metrics)

- **Memory Usage (Heap Usage):** `[e.g., Executors' heap reaches 90% and fails]`
- **CPU Usage:** `[e.g., CPU remains low (<10%) most of the time (possible Driver or I/O bottleneck)]`
- **Disk:** `[e.g., Was "Spill to Disk" detected?]`

---

## 3. Critical Code Snippet

*(Below is the PySpark code snippet corresponding to the slow stage or where the error occurs)*

```python
# [PASTE THE RELEVANT SNIPPET OF YOUR SCRIPT HERE]
# e.g.,
# df_final = df_a.join(df_b, "id", "left").groupBy("region").agg(sum("valor"))
# df_final.write.mode("overwrite").parquet("s3://...")
```

---

## 4. Optimization Request

Based on the logs and context above, please provide:

1. **Root Cause Diagnosis:**
   - Technically explain the reason for the failure or slowness (e.g., Data Skew, Driver OOM, Small Files).

2. **Code Solution (Refactoring):**
   - Rewrite the snippet above, applying the necessary corrections (e.g., Salting, Coalesce, Broadcast).

3. **Spark Tuning (`--conf`):**
   - Which specific parameters should I configure? (e.g., `spark.sql.shuffle.partitions`, `spark.memory.fraction`).

4. **Infrastructure Recommendation:**
   - Should I change the worker type (G.1X vs. G.2X) or the quantity?
