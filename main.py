import os
import openai
import json
from database import Database
import asyncio

from dotenv import load_dotenv

load_dotenv("/home/ec2-user/analyze-address-cost/dot.env")
# load_dotenv("dot.env")


async def get_openai_response(prompt):
    OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
    CHATGPT_MODEL = os.getenv("CHATGPT_MODEL")

    client = openai.OpenAI(api_key=OPENAI_API_KEY)
    result=""

    try:
        response = await client.chat.completions.create(
            model=CHATGPT_MODEL,
            messages=[{"role": "user", "content": prompt}]
        )
        result = response.choices[0].message.content
    except Exception as e:
        print("Exception: ", e)

    return result


async def get_cost(neighborhood_address):
    cost_prompt = neighborhood_address+"\nGive me accurate cost in dollar per meter square for given address and provide response in {'cost': actual_cost} format only. If actual_cost not found than {'cost': 0, 'error': error}. Return the JSON formatted with {} and don't wrap with ```json."

    response = await get_openai_response(cost_prompt)
    if type(response) != "json":
        try:
            response = json.loads(response.replace("'", "\""))
        except:
            response = {"cost": 0}

    return response["cost"]


async def get_neighbourhood_address(full_address):
    neighborhood_prompt = full_address+"\nGive me neighborhood address from given address and provide response in {'address': neighborhood_address} format only. If neighborhood_address not found than {'address': '', 'error': error}. \nReturn the JSON formatted with {} and don't wrap with ```json.\nneighborhood_address should not contains single quote and apostrophe s. neighborhood_address must be in a string."

    response = await get_openai_response(neighborhood_prompt)
    print("response: ", response)
    if type(response) != "json":
        try:
            response = json.loads(response.replace("'", "\""))
        except:
            response = {"address": ""}

    return response["address"]


async def calculate_cost():
    db = Database()
    records = db.read_user_data()
    data = []

    for row in records:
        data = []
        accountid = row["accountid"]
        country = row["country"]
        city = row["city"]
        address = row["address"]

        full_address = "country="+country+", city="+city+", address="+address
        address = country+", "+city+", "+address

        neighborhood_address = await get_neighbourhood_address(full_address)

        cost_1 = await get_cost(neighborhood_address)
        data.append((accountid, neighborhood_address, cost_1))

        cost_2 = await get_cost(full_address)
        data.append((accountid, address, cost_2))

        db.insert_data(data)

    db.read_cost_data()


if __name__ == "__main__":
    asyncio.run(calculate_cost())

