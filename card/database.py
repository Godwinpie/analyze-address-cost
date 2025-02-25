import os
import pymssql
from dotenv import load_dotenv

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

load_dotenv(os.path.join(BASE_DIR, "dot.env"))

server = os.getenv("DB_HOST")
database = os.getenv("DB_NAME")
username = os.getenv("DB_USER")
password = os.getenv("DB_PASS")

class Database:
    def __init__(self):
        self.conn = pymssql.connect(server, username, password, database, as_dict=True)

    def read_card_data(self):
        query = """select card_id from [dbo].[praxis_extended] a where card_id is not null and card_type is null
            and not exists (select 1 from [report].[card_index] b WHERE a.card_id = b.card_id) group by card_id;
        """
        cursor = self.conn.cursor()
        cursor.execute(query)

        records = cursor.fetchall()
        print("Card records: ", len(records))
        return records
    
    def insert_card_details(self, data):
        cursor = self.conn.cursor()
        query = "INSERT INTO [report].[card_index] (card_id, product_name) VALUES (%s, %s)"
        cursor.executemany(query, data)
        self.conn.commit()
        cursor.close()

        print("Data inserted")
    
    def read_card_data_from_card_index(self):
        query = """select card_id, product_name from [report].[card_index]"""
        cursor = self.conn.cursor()
        cursor.execute(query)

        records = cursor.fetchall()
        # print('records: ', len(records))

        for record in records:
            print(record)
        return records


# db = Database()
# db.read_card_data()
# db.read_card_data_from_card_index()
