import os
from dotenv import load_dotenv
import pymssql

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, "dot.env"))

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
            where first_deposit_date>'2024-01-01' and city is not null
            and not exists (select 1 from [dbo].[client_location_cost] b WHERE a.accountid = b.accountid);
        """

        cursor = self.conn.cursor()
        cursor.execute(query)

        records = cursor.fetchall()
        print("Client records: ", len(records))
        return records

    def insert_data(self, data):
        cursor = self.conn.cursor()
        query = "SELECT COUNT(*) AS TOTAL FROM [dbo].[client_location_cost] WHERE accountid = %s"
        cursor.execute(query, data[0][0])
        records = cursor.fetchall()

        if records[0]["TOTAL"] > 0:
            query = f"UPDATE [dbo].[client_location_cost] SET client_neighborhood={data[0][1]}, cost_per_sqm={data[0][2]}, object={data[0][3]}, area_type={data[0][4]}, people_type={data[0][5]}, property_type={data[0][6]}, is_valid={data[0][7]} WHERE accountid = {data[0][0]};"
            cursor.execute(query)
            self.conn.commit()
        else:
            query = "INSERT INTO [dbo].[client_location_cost] (accountid, client_neighborhood, cost_per_sqm, object, area_type, people_type, property_type, is_valid) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
            cursor.executemany(query, data)
            self.conn.commit()
        cursor.close()

        print("Data inserted")
    
    def read_cost_data(self):
        # query = """SELECT COUNT(*) AS TOTAL FROM [dbo].[client_location_cost]"""
        query = """SELECT * FROM [dbo].[client_location_cost]"""

        cursor = self.conn.cursor()
        cursor.execute(query)

        records = cursor.fetchall()
        #print('records: ', records)

        for record in records:
            print('record: ', record["people_type"], record["area_type"], record["is_valid"])

    def remove_duplicate_records(self):
        query = """SELECT accountid, COUNT(*) AS RecordCount FROM [dbo].[client_location_cost] GROUP BY accountid;"""

        cursor = self.conn.cursor()
        cursor.execute(query)

        records = cursor.fetchall()

        for record in records:
            if record['RecordCount'] > 1:
                query = """DELETE FROM [dbo].[client_location_cost] WHERE accountid = %s;"""
                cursor.execute(query, record['accountid'])
                self.conn.commit()
                print('Deleted record: ', record['accountid'])

    def get_fields(self, table_name):
        query = f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = {table_name}"
        cursor = self.conn.cursor()
        cursor.execute(query)

        records = cursor.fetchall()
        print('records: ', records)

db = Database()
# db.read_user_data()
# db.read_cost_data()
# db.get_fields('client_location_cost')
# db.remove_duplicate_records()
