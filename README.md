# Periodic Snapshot Fact Table Design
Medium article: https://medium.com/@joyo.development/periodic-snapshot-table-design-5f78036ecd54

Periodic snapshot table is one of the four types of fact tables in Data Warehousing.

It contains periodic data from some time interval like day, week, month etc.

In this example we track customers activity on some web portal on weekly basis in minutes. 

Let’s say that our customers get maximum of 600 minutes (10 hours) to use portal every week. We want to track the remaining time for every customer at the end of the week, to see how much our portal is being used.

Full Data Warehouse schema is given on the image below:

<img src="https://github.com/AleX77NP/Periodic-snapshot-table-design/raw/main/images/schema.png">

We have two dimension tables - `CUSTOMER_DIM` and `WEEK_DIM`, and our Periodic snapshot fact table 
`CUSTOMER_USAGE_EOW_SNAPSHOT` (EOW = End of week).

`Customer_Usage_Activity_EOW_Minutes_Remaining` column in fact table represents number of remaining minutes customer gets to spend on portal during that week.

This type of measure is called semi-additive measure, because it cannot it’s not additive — we cannot just sum up all remaining minutes from every week and get total, because this value resets to 600 at the beginning of every new week. However, we can perform other numeric operations, calculate things like average remaining minutes per customer across weeks, average remaining minutes for every week or total remaining minutes for specific week. This is why this type of table has “Snapshot” in its name.

## Setup
First, we need to create tables in our data warehouse, and populate our dimension tables for customers and weeks.
Here Sqlite is chosen for simplicity reasons.
In order to setup data warehouse and tables, do the following:
1. Run `python3 warehouse_setup.py` to create tables
2. Run `python3 populate_dimensions.py` to insert 5 users from `sql/` folder and all weeks for 2021 ad 2022

## Pipeline
File `pipeline.py` is a PySpark script that takes new weekly data from `new_data.csv` file and prepares
data with `GROUP BY` (by user and week) and `SUM` on daily activities in order to create weekly rows, since csv data
is daily data. Then we need to subtract that sum from 600 to get remaining minutes for the week for every customer.

We save this result inside the `STAGING` table which is only used for this temporary data, and is deleted 
after pipeline is finished.

Finally, we use `LEFT OUTER JOIN` on CUSTOMER_DIM table to insert weekly data for every user.
We want to insert data for every user, even if some user is not present in new data from csv file, 
this is why `LEFT OUTER JOIN` is needed (if there was no activity by user - `NULL`, 600 will be inserted).

We also need `WHERE NOT EXISTS` clause in our query to make sure no duplicates are inserted (We check if there exists
Fact Row with same Customer_Key & Week_Key combination). 
(You can run pipeline multiple times and see that same data will not be inserted).

Of course, we need to use `COALESCE` since data in STAGING table will be NULL if user was not present/active
during the past week.

## DAG
- Pipeline can be run from Airflow as a DAG using SparkSubmitOperator.
- Dag is located inside `dags/` directory.

### Notes:
- Make sure sqlite driver is present inside `drivers/` directory or some other location of your choice.
This is needed for Spark's JDBC operations.
- How to setup Apache Airflow with Spark: https://anant.us/blog/modern-business/airflow-and-spark-running-spark-jobs-in-apache-airflow/

