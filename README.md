# MedFlux

## **Overview**
This project involves creating a real-time healthcare analytics dashboard using **Python Dash**, **MySQL**, and **AWS S3**. The dashboard allows healthcare providers to explore **demographics, disease trends, and treatment costs**. An **ETL pipeline** built with Python fetches and processes healthcare records from AWS S3 and loads them into a MySQL database.

---

## **Table of Contents**
1. [Technologies Used](#technologies-used)
2. [Directory Structure](#directory-structure)
3. [Features](#features)
4. [Installation and Setup](#installation-and-setup)
5. [Configuration](#configuration)
6. [Usage](#usage)
7. [Sample Queries](#sample-queries)
8. [Future Enhancements](#future-enhancements)

---

## **Technologies Used**
- **Python** (Dash, Pandas, SQLAlchemy, Boto3)
- **MySQL**
- **AWS S3** (for data storage)

---

## **Directory Structure**
```
/healthcare-analytics/
├── etl_pipeline.py   # ETL pipeline to load data from AWS S3 into MySQL
├── app.py            # Dash app for real-time dashboard
├── config.py         # Configuration for AWS and MySQL credentials
└── requirements.txt  # Dependencies
```

---

## **Features**
1. **ETL Pipeline:**
   - Fetches data from AWS S3.
   - Processes and cleans the data.
   - Loads records into a MySQL database.
2. **Real-Time Dashboard:**
   - **Disease Trend Visualization:** Displays diagnosis counts over time.
   - **Cost Analysis:** Shows the total treatment cost by region.
   - **Filter by Region:** Allows filtering by geographical region.

---

## **Installation and Setup**

### **1. Clone the Repository**
```bash
git clone <repository-url>
cd healthcare-analytics
```

### **2. Install Dependencies**
Create a virtual environment (optional):
```bash
python3 -m venv venv
source venv/bin/activate   # For Linux/Mac
venv\Scripts\activate    # For Windows
```
Install dependencies:
```bash
pip install -r requirements.txt
```

### **3. Create MySQL Database and Table**
1. **Create Database:**
   ```sql
   CREATE DATABASE healthcare_db;
   ```
2. **Create Table:**
   ```sql
   CREATE TABLE inpatient_records (
       id INT AUTO_INCREMENT PRIMARY KEY,
       patient_id VARCHAR(255),
       age INT,
       gender VARCHAR(10),
       admission_date DATE,
       discharge_date DATE,
       diagnosis VARCHAR(255),
       procedure VARCHAR(255),
       cost DECIMAL(10, 2),
       region VARCHAR(255)
   );
   ```

---

## **Configuration**
Edit the `config.py` file and update your credentials:
```python
# AWS S3 Configuration
AWS_ACCESS_KEY = "your-aws-access-key"
AWS_SECRET_KEY = "your-aws-secret-key"
S3_BUCKET_NAME = "your-s3-bucket-name"
S3_FOLDER_PATH = "incoming-data/"

# MySQL Configuration
MYSQL_USERNAME = "your-mysql-username"
MYSQL_PASSWORD = "your-mysql-password"
MYSQL_HOST = "localhost"
MYSQL_PORT = "3306"
MYSQL_DB = "healthcare_db"
```
Ensure that your AWS credentials have the necessary permissions (`s3:GetObject`, `s3:ListBucket`).

---

## **Usage**

### **1. Run the ETL Pipeline**
Execute the ETL script to process new records from S3 and load them into MySQL:
```bash
python etl_pipeline.py
```

### **2. Run the Dash App**
Start the real-time dashboard:
```bash
python app.py
```
Visit `http://127.0.0.1:8050` in your browser to access the dashboard.

---

## **Sample Queries**
Below are some sample SQL queries for additional data exploration:

1. **Total Records by Region:**
   ```sql
   SELECT region, COUNT(*) AS record_count FROM inpatient_records GROUP BY region;
   ```
2. **Top 5 Most Common Diagnoses:**
   ```sql
   SELECT diagnosis, COUNT(*) AS count FROM inpatient_records GROUP BY diagnosis ORDER BY count DESC LIMIT 5;
   ```
3. **Total Cost by Date:**
   ```sql
   SELECT admission_date, SUM(cost) AS total_cost FROM inpatient_records GROUP BY admission_date;
   ```

---

## **Future Enhancements**
1. Add user authentication for secure data access.
2. Add geographic heatmaps to visualize regional distributions.
3. Enable export of filtered data as CSV for reporting.
4. Integrate automated alerts for anomalies in cost trends.

---

## **Troubleshooting**
- **Connection Issues:** Verify that MySQL is running and that credentials in `config.py` are correct.
- **AWS Access Denied:** Check your AWS IAM permissions.
- **S3 File Not Found:** Ensure that files are uploaded to the correct folder in the S3 bucket.

---

## Contributors
- **Nikhil Giridharan**

---

This project provides a comprehensive healthcare analytics dashboard by automating data ingestion and supporting real-time analysis for better decision-making.

