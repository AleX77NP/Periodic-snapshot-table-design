import findspark
from datetime import datetime
from warehouse_handler import DataWarehouseHandler
from constants import DB_LOCATION, JAR_PATH

findspark.init()

from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.functions import sum as _sum
from pyspark.sql.types import IntegerType


# function that gets week number from date (string)
def getWeekNumber(date):
    date_obj = datetime.strptime(date, '%Y-%m-%d')
    week_num = date_obj.isocalendar().week
    return week_num


weekUDF = udf(lambda x: getWeekNumber(x), IntegerType())

# Pipeline main code --------------------------------------------------------------------------------------------------

dw_handler = DataWarehouseHandler(DB_LOCATION)

conf = SparkConf()  # create the configuration
conf.set('spark.jars', JAR_PATH)
# set spark.jars

# init spark session
spark = SparkSession.builder \
    .config(conf=conf) \
    .appName('Customer activity analysis') \
    .getOrCreate()

# load daily batch of data that represents user activity on specific day
df = spark.read.options(inferSchema='True', delimiter=',', header='True') \
    .csv('./new_data.csv')

df = df.withColumn('Week_Number', weekUDF(col('Date')))

# data should be grouped by customer email and week
group_cols = ['Customer_Email', 'Week_Number']

# sum up all activity for for every customer by week
df = df.groupBy(group_cols) \
    .agg(_sum('Activity_Minutes').alias('Week_Activity_Minutes')) \
    .orderBy('Week_Number')

# df.show()

# write data into staging table
df.write.mode('append').format('jdbc'). \
    options(url=f'jdbc:sqlite:{DB_LOCATION}',
            driver='org.sqlite.JDBC', dbtable='STAGING', overwrite=True).save()

current_year = datetime.now().year  # current year to restrict JOIN on week's natural key
past_week = 58  # 5th week of the year 2022 so csv data is past week's data

# here CUSTOMER_DIM is our main driver for the query, since we want to insert row even if there was no activity
# we want to do LEFT OUTER JOIN on STAGING and WEEK_DIM tables to get proper foreign keys for our fact row
# WHERE NOT EXISTS clause makes sure that we don't insert any duplicates
# only assumption is that no new user is in the new data, that should be covered via SCD before this
sql_query = f"""
        INSERT INTO CUSTOMER_USAGE_EOW_SNAPSHOT (Customer_Key, Week_Key, Customer_Usage_EOW_Minutes)
        SELECT c.Customer_Key,
               COALESCE(w.Week_Key, {past_week}),
               COALESCE(s.Week_Activity_Minutes, 0)
        FROM CUSTOMER_DIM c
        LEFT OUTER JOIN STAGING s ON
            c.Customer_Email = s.Customer_Email
        LEFT OUTER JOIN WEEK_DIM w
            ON s.Week_Number = w.Week_Number
            AND w.Year = {current_year}
        WHERE NOT EXISTS (
            SELECT 1 FROM CUSTOMER_USAGE_EOW_SNAPSHOT f
            WHERE f.Customer_Key = c.Customer_Key
            AND f.Week_Key = COALESCE(w.Week_Key, {past_week})
        )     
    ;"""

# insert fact rows and delete data from staging table
dw_handler.insert_fact(sql_query)
dw_handler.cleanup()
