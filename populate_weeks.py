import sqlite3


def insert_weeks():
    wh_connection = sqlite3.connect('customer_analysis.db')
    cursor = wh_connection.cursor()

    years = [2021, 2022]
    weeks = [*range(1, 54, 1)]  # weeks 1-53

    for year in years:
        for week in weeks:
            sql = ''' INSERT INTO WEEK_DIM (Week_Number, Year) VALUES (?, ?) '''
            cursor.execute(sql, (week, year))
            wh_connection.commit()

    wh_connection.close()


insert_weeks()
