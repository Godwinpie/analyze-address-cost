import os
from dotenv import load_dotenv

import httpx
import asyncio
from database import Database

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
load_dotenv(os.path.join(BASE_DIR, "dot.env"))

server = os.getenv("DB_HOST")
database = os.getenv("DB_NAME")
username = os.getenv("DB_USER")
password = os.getenv("DB_PASS")


async def get_card_type(card_id):
    request = httpx.AsyncClient()
    url="https://api.iinlist.com/cards"
    params = {
        "iin": card_id
    }
    headers = {
        "X-API-Key": os.getenv("CARD_API_KEY")
    }
    response = await request.get(url, params=params, headers=headers)
    response = response.json()
    
    name=""
    if response and len(response["_embedded"]["cards"]) > 0:
        if "name" in response["_embedded"]["cards"][0]["product"].keys():
            name = response["_embedded"]["cards"][0]["product"]["name"]
            print('=>', card_id+" - "+name)

    return name


async def manage_card():
    db = Database()
    records = db.read_card_data()

    for row in records:
        data = []
        card_id = row["card_id"]

        card_type = await get_card_type(card_id)
        if len(card_type) > 0:
            data.append((card_id, card_type))
            db.insert_card_details(data)

    db.read_card_data_from_card_index()


if __name__ == "__main__":
    asyncio.run(manage_card())

