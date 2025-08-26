# Sparkify Data Lake with Spark

## 📌 Project Overview
A startup called **Sparkify** wants to analyze the data they’ve been collecting on songs and user activity from their new music streaming app.  

Currently, the data is stored in:  
- **JSON logs** containing user activity events.  
- **JSON metadata** containing information about songs.  

The analytics team is particularly interested in **understanding what songs users are listening to**, but the raw JSON data is not optimized for analysis.  

To solve this, we built a **data lake solution** using **Apache Spark**.  
- The ETL pipeline reads data from **AWS S3**.  
- Transforms it into a **star schema**.  
- Writes the results back as **partitioned parquet files** for efficient querying.  

---

## 🚀 How to Run

Run the ETL pipeline with:  

```bash
python etl.py
