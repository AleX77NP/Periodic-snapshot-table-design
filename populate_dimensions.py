import sqlite3


def insert_weeks(cursor):
    years = [2021, 2022]
    weeks = [*range(1, 54, 1)]  # weeks 1-53

    sql = ''' INSERT INTO WEEK_DIM (Week_Number, Year) VALUES (?, ?);'''
    week_rows = []

    for year in years:
        for week in weeks:
            week_rows.append((week, year))

    # insert all weeks for years 2021 and 2022
    cursor.executemany(sql, week_rows)


def insert_customers(cursor):
    fd = open('sql/customers.sql', 'r')
    sql = fd.read()
    fd.close()

    cursor.executescript(sql)


def insert_dimension_data():
    wh_connection = sqlite3.connect('customer_analysis.db')
    cursor = wh_connection.cursor()

    insert_weeks(cursor)
    insert_customers(cursor)

    wh_connection.commit()
    wh_connection.close()


# insert data for dimension tables
insert_dimension_data()
