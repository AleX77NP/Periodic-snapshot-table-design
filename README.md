# Periodic Snapshot Fact Table Design
Periodic snapshot table is one of the three types of fact tables in Data Warehousing.

It contains periodic data from some time interval like week, month etc.

In this example we track customers activity on some web portal per week in minutes. 
Full Data Warehouse schema is given on the image below:

<img src="https://github.com/AleX77NP/Periodic-snapshot-table-design/raw/main/images/schema.png">

We have two dimension tables - `CUSTOMER_DIM` and `WEEK_DIM`, and our Periodic snapshot fact table 
`CUSTOMER_USAGE_EOW_SNAPSHOT` (EOW = End of week).

## Setup
First, we need to create tables in our data warehouse, and populate our dimension tables for customers and weeks.
Here Sqlite is chosen for simplicity reasons.
In order to setup data warehouse and tables, do the following:
1. Run `python3 warehouse_setup.py` to create tables
2. Run `python3 populate_dimensions.py` to insert 5 users from `sql/` folder and all weeks for 2021 ad 2022

## Pipeline
File `pipeline.py` is PySpark script that takes new weekly data from `new_data.csv` file and prepares
data with operations such as `GROUP BY` and `SUM` in order to create weekly rows, since csv data
is daily data.

We save this result inside the `STAGING` table which is only used for this temporary data, and is deleted 
after pipeline is finished.

Finally, we use `LEFT OUTER JOIN` on CUSTOMER_DIM table to insert weekly data for every user.
We want to insert data for every user, even if some user is not present in new data from csv file, 
this is why `LEFT OUTER JOIN` is needed (if there was no activity by user - `NULL`, 0 will be inserted).

We also need `WHERE NOT EXISTS` clause in our query to make sure no duplicates are inserted (We check if there exists
Fact Row with same Customer_Key & Week_Key combination). 
(You can run pipeline multiple times and see that same data will not be inserted).

## DAG
- Pipeline can be run from Airflow as a DAG using SparkSubmitOperator.
- Dag is located inside `dags/` directory.

### Notes:
- Make sure sqlite driver is present inside `drivers/` directory or some other location of your choice.
This is needed for Spark's JDBC operations.
- How to setup Apache Airflow with Spark: https://anant.us/blog/modern-business/airflow-and-spark-running-spark-jobs-in-apache-airflow/

