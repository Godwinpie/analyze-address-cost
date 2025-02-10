import os
from dotenv import load_dotenv
import pymssql

load_dotenv("dot.env")

server = os.getenv("DB_HOST")
database = os.getenv("DB_NAME")
username = os.getenv("DB_USER")
password = os.getenv("DB_PASS")

conn = pymssql.connect(
    server=server,
    user=username,
    password=password,
    database=database,
    as_dict=True
)

class Database:

    def __init__(self):
        self.conn = pymssql.connect(server, username, password, database, as_dict=True)

    def create_table(self):
        try:
            create_table_query="""
                CREATE TABLE report.address_cost (
                ID INT IDENTITY(1,1) PRIMARY KEY,
                cost_per_meter_square INT NOT NULL,
                address varchar(255),
                accountid INT,
                CONSTRAINT FK_accountid FOREIGN KEY (accountid) REFERENCES report.vtiger_account(accountid) ON DELETE CASCADE)
            """
            cursor = self.conn.cursor()
            cursor.execute(create_table_query)
            conn.commit()

            print("Table created.")

        except pymssql.DatabaseError as e:
            print("Error:", e)

    def read_user_data(self):
        query="""select accountid, country, city, address from report.vtiger_account a 
            where first_deposit_date>'2024-01-01' and city is not null and
            not exists (select 1 from [dbo].[client_location_cost] b WHERE a.accountid = b.accountid);
        """

        cursor = self.conn.cursor()
        cursor.execute(query)

        records = cursor.fetchall()
        return records

    def insert_data(self, query, data):
        cursor = self.conn.cursor()
        cursor.executemany(query, data)

        self.conn.commit()
        cursor.close()

        print("Data inserted")
    
    def read_cost_data(self):
        query = """SELECT TOP (1000) [accountid]
                ,[client_neighborhood]
                ,[cost_per_sqm]
                FROM [dbo].[client_location_cost]"""

        cursor = self.conn.cursor()
        cursor.execute(query)

        records = cursor.fetchall()
        print('records: ', records)
