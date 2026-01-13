# 💡 Spark Event Log Parser (v0.1)

Parse Spark application event logs (JSON-lines) and generate execution-level performance report:
- SQL execution -> jobs -> stages -> tasks
- Slowest stages / tasks
- Basic skew suspects (based on duration vs median)
- Slowest scanned files (based on plan metrics)
- Scan parquet metrics (rows + scan time) and filter selectivity


## 📂 Repository structure
```text
spark-eventlog-parser/
    src/
        spark_eventlog_parser.py
    notebooks/
        demo.ipynb
```

## 💫 Quick start

See *notebooks/demo.ipynb* for an example

The notebook demonstrates:
- Parsing a Spark event log
- Selecting a Spark **execution id**
- Printing an execution overview and detailed trace



## 📜 Output
The report prints:
 - **Block 1** - overview (top slowest stages/tasks, skew suspects. slowest files)
 - **Block 2** - detailed trace (jobs => stages => tasks => metrics)



## ⚠️ Notes / Limitations (v0.1)a

- Reporting is organized by Spark SQL execution Id (from *SparkListenerSQLExecutionStart / End*)
- *SparkListenerJobStart*.Stage IDs may include stages that never get submitted. There are reported as **missing/skipped**
- Stage-to-job association is heuristic (based on JobStart Stage IDs and submission order)


## ☝ Project status
This is a first version intended for portfolio/demo purposes.


