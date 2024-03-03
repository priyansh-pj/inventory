from fastapi import FastAPI
from database import Database

database = Database()
app = FastAPI()

@app.on_event("startup")
async def startup():
    await database.connect()
    await database.insert_csv_data()

@app.on_event("shutdown")
async def shutdown():
    await database.disconnect()
@app.get("/inventory")
async def root():
    return await database.fetch_products()
@app.get("/inventory/{product}")
async def say_hello(product: str):
    return await database.fetch_product(product)
