# 📌 Pinterest Data Pipeline

## Overview
This project processes Pinterest data by ingesting, transforming, and analyzing it using **Kafka, MySQL, Databricks, and AWS MWAA and AWS Kinesis**. Initially, the data was retrieved from AWS S3, and sent to kafka topics via an API gatway only then to be finally loaded in to databricks (Milestone 1-6). Due to access restrictions,intial data was later moved to a **local MySQL database** and instead of using kafka data was imported directly to databricks workspace. Pinterest Data.ipynb note book was split in to several jobs which where triggered in order governed by **Apache Airflow** dag file (Milestone 7). **AWS MWAA** and **AWS Kinesis** were not used (what would have been milestone 7-8).

## Project Workflow
1. **Data Ingestion:** Data is sourced from AWS S3 (Milestone 1-6) to databricks or a local SQL database (Milestone 7).
2. **Streaming with Kafka:** Data is streamed via Kafka topics (Milestone 1-6).
3. **Data Transformation:** Transformation using Databricks notebooks.
4. **Data Anayltics:** Obtaining useful insights from the data. 
5. **Orchestration:** User runs the files manually for Milestones 1-6 and for milestone 7 Airflow DAG schedules and monitors the pipeline.

---

# 📌 Milestone 1: Environment Setup
- Install Python and required libraries.
- Install MySQL for local database (for Milestone 7 onwards).
- Set up Databricks and Kafka.

---

# 📌 Milestone 2: Data Download & AWS Login
- **Download Pinterest dataset** from **AWS S3**.
- **Log in** to AWS using IAM credentials.

### Execution Steps
1. Navigate to **AWS Console** and **download the dataset**.
2. Import the dataset into **S3 storage**.

---

# 📌 Milestone 3: EC2 Kafka Setup
- **Set up an EC2 instance**.
- **Install Apache Kafka** and create topics.

### Execution Steps
1. **Connect to EC2 instance:**
   ```bash
   ssh -i my-key.pem ubuntu@ec2-instance-ip
   ```
2. **Start Kafka server:**
   ```bash
   bin/zookeeper-server-start.sh config/zookeeper.properties &
   bin/kafka-server-start.sh config/server.properties &
   ```
3. **Create Kafka topics:**
   ```bash
   bin/kafka-topics.sh --create --topic pin --bootstrap-server localhost:9092
   bin/kafka-topics.sh --create --topic geo --bootstrap-server localhost:9092
   bin/kafka-topics.sh --create --topic user --bootstrap-server localhost:9092
   ```

---

# 📌 Milestone 4: API Gateway for Kafka
- Implement **Kafka REST API**.
- Modify `user_posting_emulation.py` to send data to Kafka.

### Execution Steps
1. **Create db_creds.yaml in safe folder where you saved user_posting_emulation.py, make sure all the credentials have been saved.**
2. **Run the Kafka producer:**
   ```bash
   python user_posting_emulation.py
   ```
3. **Verify data arrival in Kafka:**
   ```bash
   bin/kafka-console-consumer.sh --topic pin --from-beginning --bootstrap-server localhost:9092
   ```

---

# 📌 Milestone 5-6: Load & Transform Data in Databricks
- Use `Pinterest Data.ipynb` for data loading and analytics.

### Execution Steps
1. Run **Pinterest Data.ipynb** in **Databricks**.

---

# 📌 Milestone 7: Switch to Local SQL Database
- **AWS Access Revoked**, switched data source to **local MySQL database**.
- **Database imported** from **AI Core**.

### Execution Steps
1. **Import data into MySQL database**.
2. **Import user, geo, and Pinterest data into Databricks Catalog**.
3. **Download Pipeline-Tasks** folder to databricks.
4. **In airflow folder create dags folder.**
5. **Download dag_run_databricks_pipepline.py in to the airflow folder.**
6. Get the **databricks instance -environment url** and **databrics access token** and store those in **config.yaml** file in airflow dags folder. 
7. **Run the Airflow DAG** to execute Databricks jobs in sequence:
   ```bash
   airflow scheduler &
   airflow webserver --port 8080
   ```
8. **Access Airflow Web UI**: Open [http://localhost:8080](http://localhost:8080) in a web browser.

---

# 📌 Apache Airflow Setup & Execution

## Installation
1. **Install Airflow**:
   ```bash
   pip install apache-airflow
   ```
2. **Initialize Airflow database**:
   ```bash
   airflow db init
   ```
3. **Create an Airflow user**:
   ```bash
   airflow users create --username admin --password admin --firstname admin --lastname admin --role Admin --email admin@example.com
   ```

## Running Airflow
1. **Start the Airflow scheduler**:
   ```bash
   airflow scheduler &
   ```
2. **Start the Airflow webserver**:
   ```bash
   airflow webserver --port 8080
   ```
3. **Access Airflow** at [http://localhost:8080/](http://localhost:8080/).

![Airflow Setup](file-VmaCBKdeSRYpVxm4gAM9Mh)

---

# 📌 Airflow DAG Workflow
The Airflow DAG (`databricks_run_pinterest_analytics_pipeline_dag`) runs **Databricks jobs** in sequence.

### **Execution Order**
![image](https://github.com/user-attachments/assets/787abd29-b6df-42a0-84a5-27a53081bb77)
1. **Trigger Jobs**:
   -  `trigger_load_transform_geo_data_job`
   - `trigger_load_transform_pinterest_data_job`
   - `trigger_load_transform_user_data_job`
2. **Wait Jobs**:
   - `wait_for_load_transform_geo_data_job`
   - `wait_for_load_transform_pinterest_data_job`
   - `wait_for_load_transform_user_data_job`
3. **Final Job**:
   - `trigger_load_transform_analytics_job`
  


---

# 📌 Configuration Files

### `config.yaml` (Required for Airflow DAG)
This file contains **Databricks instance credentials**.

```yaml
databricks:
  instance: "https://databricks-instance-url"
  token: "your-databricks-token"
```

### `db_creds.yaml` (Required for Database Connections)
This file is used in `user_posting_emulation.py`.

```yaml
database_login:
  host: "localhost"
  user: "root"
  password: "your-password"
  database: "pinterest_db"
  port: 3306
```

---

# 📌 Project Summary
- **Milestones 1-6:** Data was **retrieved from AWS S3** and sent to **Kafka topics**.Data was then retrived in databrick in python dataframe in the Pinterest Data.ipynb file. 
- **Milestone 7:** AWS server **was not available**, so data was **imported into a local SQL repository**.
- **Data transformation** was handled using **Databricks notebooks**: Data.ipynb for Milestone 1-6 and Pipeline-Tasks folder notebooks for Milestone 7.
- **Data Anayltics:** was also handled using **Databricks notebooks**: Data.ipynb for Milestone 1-6 and Load_All_Data_Perform_Analytics.ipynb for Milestone 7. 
- **Apache Airflow** was used for **workflow orchestration** for Milestone 7.

---
