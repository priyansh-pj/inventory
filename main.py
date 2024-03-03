from fastapi import FastAPI
from database import Database
import math
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
async def fetch_product(product: str):
    return await database.fetch_product(product)

@app.get("/prediction/{product}")
async def fetch_prediction(product: str):
    datas = await database.fetch_prediction_product(product)
    qty = 0
    if len(datas) > 0:
        for data in datas:
            qty += abs(data.get('opening_stock') - data.get('net_qty') - data.get('closing_stock'))
        qty = math.ceil(qty/len(datas))
        datas[0]['prediction'] = qty
        return datas[0]
    return {"message": "Not found"}