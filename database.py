import asyncpg
from asyncpg.pool import Pool
import csv
class Database:
    def __init__(self):
        self.pool: Pool = None

    async def connect(self):
        self.pool = await asyncpg.create_pool(user='postgres', database='canteen')

    async def disconnect(self):
        if self.pool:
            await self.pool.close()

    async def execute(self, query: str, *args):
        async with self.pool.acquire() as connection:
            return await connection.execute(query, *args)
    async def insert_csv_data(self):
        try:
            # Connect to PostgreSQL database
            conn = await asyncpg.connect(user='postgres', database='canteen')

            # Check if the table exists
            table_exists = await conn.fetchval("""
                SELECT EXISTS (
                    SELECT 1 
                    FROM information_schema.tables 
                    WHERE table_name = 'canteen'
                )
            """)

            # If the table does not exist, create it
            if not table_exists:
                await conn.execute('''
                    CREATE TABLE canteen (
                        id SERIAL PRIMARY KEY,
                        month VARCHAR(256),
                        year INTEGER,
                        gp_index VARCHAR(256),
                        pluno VARCHAR(256),
                        item_name VARCHAR(256),
                        net_qty INTEGER,
                        opening_stock INTEGER,
                        closing_stock INTEGER
                    )
                ''')

                print("Table 'canteen' created successfully")

            # If the table exists, skip data insertion
            else:
                print("Table 'canteen' already exists. Skipping data insertion.")

            # If the table was created or already existed, proceed with data insertion
            if not table_exists:
                # Open the CSV file
                with open('combined_data.csv', 'r') as file:
                    reader = csv.DictReader(file)

                    # Iterate over each row in the CSV file
                    for row in reader:
                        # Extract data from the CSV row
                        month = row['Month']
                        year = int(row['Year'])
                        gp_index_no = row['GP_Index_No']
                        pluno = row['pluno']
                        item_name = row['Item_Name']
                        net_qty = int(row['Net_Qty'])
                        opening_stock = int(row['O_B'])
                        closing_stock = int(row['Closing_Stock'])

                        # Execute SQL INSERT statement
                        await conn.execute("""
                            INSERT INTO canteen (month, year, gp_index, pluno, item_name, net_qty, opening_stock, closing_stock)
                            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                        """, month, year, gp_index_no, pluno, item_name, net_qty, opening_stock, closing_stock)

                print("CSV data inserted successfully")
        finally:
            # Close the database connection
            if conn:
                await conn.close()

    async def fetch_products(self):
        try:
            conn = await asyncpg.connect(user='postgres', database='canteen')
            rows = await conn.fetch("SELECT * FROM canteen ORDER BY year DESC, month DESC")

            data = [dict(row) for row in rows]

            return data
        finally:
            if conn:
                await conn.close()

    async def fetch_product(self, product_id):
        try:
            conn = await asyncpg.connect(user='postgres', database='canteen')
            rows = await conn.fetch("SELECT * FROM canteen WHERE pluno = $1 ORDER BY year DESC, month DESC", product_id)

            data = [dict(row) for row in rows]

            return data
        finally:
            if conn:
                await conn.close()

    async def fetch_prediction_product(self, product_id):
        try:
            conn = await asyncpg.connect(user='postgres', database='canteen')
            rows = await conn.fetch("SELECT * FROM canteen WHERE pluno = $1 ORDER BY year DESC, month DESC LIMIT 5", product_id)

            data = [dict(row) for row in rows]

            return data
        finally:
            if conn:
                await conn.close()