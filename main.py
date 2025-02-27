import os
import openai
import json
from database import Database
import httpx
from PIL import Image
from io import BytesIO

import base64
import asyncio

from dotenv import load_dotenv

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, "dot.env"))


OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
CHATGPT_MODEL = os.getenv("CHATGPT_MODEL")
GOOGLE_MAP_API_KEY = os.getenv("GOOGLE_MAP_API_KEY")

client = openai.AsyncOpenAI(api_key=OPENAI_API_KEY)

async def get_openai_response(prompt):
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
    cost_prompt = neighborhood_address+"\nGive me accurate per meter square cost in dollar for given address and provide response in {'cost': actual_cost} format only.\nIf cost not found, than give me average cost of that location in the given response format. Do not include currency symbol in the response.\nReturn the JSON formatted with {} and don't wrap with ```json. Not include None in response."

    response = await get_openai_response(cost_prompt)
    if type(response) != "json":
        try:
            response = json.loads(response.replace("'", "\""))
        except:
            response = {"cost": 0}

    cost = response.get("cost", 0)
    if type(cost) != str:
        return cost
    else:
        return float(cost)


async def get_neighbourhood_address(full_address):
    neighborhood_prompt = full_address+"\nIn which neighborhood is this street located? and provide response in {'address': neighborhood_address} format only. If neighborhood not found than {'address': '', 'error': error}. \nReturn the JSON formatted with {} and don't wrap with ```json.\nNeighborhood should not contains single quote and apostrophe s. Neighborhood must be in a string."

    response = await get_openai_response(neighborhood_prompt)
    if type(response) != "json":
        try:
            response = json.loads(response.replace("'", "\""))
        except:
            response = {"address": ""}

    return response.get("address", "")


async def analyse_address_using_openai(address):
    response = {'object': '', 'area_type': '', 'people': '', 'property_type': ''}

    response = await client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {
                "role": "user",
                "content": [
                    {
                        "type": "text",
                        "text": "Address: "+address+"\nGive me response in this json format: {'area_type': 'which type of properties are in that area like commercial or residential', 'people': 'which type of peoples are living there like wealthy, or poor', 'property_type': 'type of property in that area like luxurius home, raw house etc'}\nReturn the JSON formatted with {} and don't wrap with ```json.",
                    }
                ],
            }
        ],
    )
    response = response.choices[0].message.content

    if type(response) != "json":
        try:
            response = json.loads(response.replace("'", "\""))
            print('address analyse: ', response)
        except:
            pass
    
    return response


async def analyse_location_image(address):
    is_valid = False
    geocode_url = "https://maps.googleapis.com/maps/api/geocode/json"
    params = {
        "address": address,
        "key": GOOGLE_MAP_API_KEY
    }
    
    geocode_request = httpx.AsyncClient()
    geocode_response = await geocode_request.get(geocode_url, params=params)
    geocode_response = geocode_response.json()

    response = {'object': '', 'area_type': '', 'people': '', 'property_type': ''}

    if geocode_response["status"] == "OK":

        location = geocode_response["results"][0]["geometry"]["location"]
        lat, lon = location["lat"], location["lng"]

        street_view_url = f"https://maps.googleapis.com/maps/api/streetview?size=600x400&location={lat},{lon}&key={GOOGLE_MAP_API_KEY}"

        street_view_request = httpx.AsyncClient()
        street_view_response = await street_view_request.get(street_view_url)

        if street_view_response.status_code == 200:
            image = Image.open(BytesIO(street_view_response.content))
            #image.save("street_view.png")
            buffered = BytesIO()
            image.save(buffered, format="PNG")

            img_str = base64.b64encode(buffered.getvalue()).decode("utf-8")
        
            response = await client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {
                        "role": "user",
                        "content": [
                            {
                                "type": "text",
                                "text": """Analyze this image and give me response in this json format: 
                                        {'object': 'detect the object', 'area_type': 'which type of property is it like commercial or residential', 'people': 'which type 
                                        of peoples are living there like wealthy, or poor', 'property_type': 'detect property type like luxurius home, raw house etc'}
                                        \nReturn the JSON formatted with {} and don't wrap with ```json. Should not contain unknow or not available in response if any information not found instead of that return any location in that city or state.
                                        """,
                            },
                            {
                                "type": "image_url",
                                "image_url": {"url": f"data:image/jpeg;base64,{img_str}"},
                            },
                        ],
                    }
                ],
            )

            response = response.choices[0].message.content

            if type(response) != "json":
                try:
                    response = json.loads(response.replace("'", "\""))
                    is_valid = True
                    print('Data: ', response)
                except:
                    print("Failed to get address analyse response")
            
        else:
            print("Failed to get location image")
    else:
        print("Failed to get latitude and longitude")

    return response, is_valid


async def calculate_cost():
    db = Database()
    records = db.read_user_data()

    for row in records:
        data = []
        accountid = row["accountid"]
        country = row["country"]
        city = row["city"]
        address = row["address"]
        original_address = ""

        if country != None and len(country) > 0:
            original_address += "country="+str(country)
        
        if city != None and len(city) > 0:
            original_address += ", city="+str(city)
        
        if address != None and len(address) > 0:
            original_address += ", address="+str(address)
        
        print("---------------------------------------------------")
        print('original_address: ', original_address)

        neighborhood_address = await get_neighbourhood_address(original_address)
        print('neighborhood_address: ', neighborhood_address)

        if len(neighborhood_address) > 0:
            cost = await get_cost(neighborhood_address)
            print('cost: ', cost)
            response, is_valid_address = await analyse_location_image(neighborhood_address)
            
            if cost > 0:
                if is_valid_address and len(response) > 0:
                    data.append((accountid, neighborhood_address, cost, response["object"], response["area_type"], response["people"], response["property_type"], 1))
                else:
                    response = await analyse_address_using_openai(neighborhood_address)
                    data.append((accountid, neighborhood_address, cost, "", response["area_type"], response["people"], response["property_type"], 1))
                db.insert_data(data)
            else:
                db.update_neighborhood_data([(accountid, neighborhood_address)])

    db.read_cost_data()


if __name__ == "__main__":
    asyncio.run(calculate_cost())

