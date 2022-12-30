import sqlite3


class DataWarehouseHandler:
    def __init__(self, db_location):
        self.db = sqlite3.connect(db_location)
        self.cursor = self.db.cursor()

    def save(self):
        self.db.commit()

    def insert_fact(self, query):
        self.cursor.execute(query)
        self.save()

    def cleanup(self):
        sql = """ DELETE FROM STAGING;"""
        self.cursor.execute(sql)
        self.save()

    def __del__(self):
        self.db.close()
