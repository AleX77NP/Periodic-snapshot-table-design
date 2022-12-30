import findspark
from datetime import datetime
from warehouse_handler import DataWarehouseHandler

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

# pipeline main code

dw_handler = DataWarehouseHandler('customer_analysis.db')

conf = SparkConf()  # create the configuration
conf.set('spark.jars', './drivers/sqlite-jdbc-3.36.0.3.jar')  # set spark.jars

# init spark session
spark = SparkSession.builder \
    .config(conf=conf) \
    .appName('Customer activity analysis') \
    .master('local') \
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
    options(url='jdbc:sqlite:customer_analysis.db',
            driver='org.sqlite.JDBC', dbtable='STAGING', overwrite=True).save()

current_year = datetime.now().year  # current year to restrict JOIN on week's natural key

sql_query = f"""
        INSERT INTO CUSTOMER_USAGE_EOW_SNAPSHOT (Customer_Key, Week_Key, Customer_Usage_EOW_Minutes)
        SELECT c.Customer_Key,
               w.Week_Key,
               s.Week_Activity_Minutes
        FROM STAGING s
        INNER JOIN CUSTOMER_DIM c ON
            s.Customer_Email = c.Customer_Email
        INNER JOIN WEEK_DIM w
            ON s.Week_Number = w.Week_Number
            AND w.Year = {current_year}
        LEFT OUTER JOIN CUSTOMER_USAGE_EOW_SNAPSHOT f
            ON c.Customer_key = f.Customer_Key
            AND w.Week_key = f.Week_Key
        WHERE f.Customer_Key is NULL
        GROUP BY c.Customer_Key,
                 w.Week_Key  
    ;"""

# insert fact rows
dw_handler.insert_fact(sql_query)
dw_handler.cleanup()
