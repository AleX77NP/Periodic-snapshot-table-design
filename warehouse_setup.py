import sqlite3
from constants import DB_LOCATION

wh_connection = sqlite3.connect(DB_LOCATION)

cursor = wh_connection.cursor()

cursor.execute("DROP TABLE IF EXISTS CUSTOMER_DIM")
cursor.execute("DROP TABLE IF EXISTS WEEK_DIM")
cursor.execute("DROP TABLE IF EXISTS CUSTOMER_USAGE_EOW_SNAPSHOT")
cursor.execute("DROP TABLE IF EXISTS STAGING")

customer_dim = """ CREATE TABLE CUSTOMER_DIM (
                   Customer_Key INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
                   Customer_Name VARCHAR(30) NOT NULL,
                   Customer_Email VARCHAR(30) NOT NULL UNIQUE,
                   Customer_Phone VARCHAR(15) NOT NULL UNIQUE
               );"""


# service_dim = """ CREATE TABLE SERVICE_DIM (
#                   Service_Key INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
#                   Service_Name VARCHAR(20) NOT NULL UNIQUE,
#                   Service_Hourly_Price SMALLINT NOT NULL
#              );"""


week_dim = """ CREATE TABLE WEEK_DIM (
               Week_Key INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
               Week_Number TINYINT NOT NULL,
               Year SMALLINT NOT NULL
           );"""


customer_usage_eow_snapshot = """ CREATE TABLE CUSTOMER_USAGE_EOW_SNAPSHOT (
                                  Customer_Key INTEGER NOT NULL,
                                  Week_Key INTEGER NOT NULL,
                                  Customer_Usage_EOW_Minutes SMALLINT NOT NULL,
                                  PRIMARY KEY(Customer_Key, Week_Key),
                                  FOREIGN KEY(Customer_Key) REFERENCES CUSTOMER_DIM(Customer_Key),
                                  FOREIGN KEY(Week_Key) REFERENCES WEEK_DIM(Week_Key)
                              );"""


staging_table = """ CREATE TABLE STAGING (
                    Staging_Key INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
                    Customer_Email VARCHAR(30) NOT NULL,
                    Week_Number TINYINT NOT NULL,
                    Week_Activity_Minutes SMALLINT NOT NULL
                );"""

cursor.execute(customer_dim)
cursor.execute(week_dim)
cursor.execute(customer_usage_eow_snapshot)
cursor.execute(staging_table)

wh_connection.close()
