import os
import openai
import json
from database import Database

from dotenv import load_dotenv

load_dotenv("dot.env")

class AskQuery:
    def __init__(self, prompt):
        self.prompt = prompt
    
    def get_response(self):
        OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
        CHATGPT_MODEL = os.getenv("CHATGPT_MODEL")

        client = openai.OpenAI(api_key=OPENAI_API_KEY)

        response = client.chat.completions.create(
            model=CHATGPT_MODEL,
            messages=[{"role": "user", "content": self.prompt}]
        )

        return response.choices[0].message.content


def calculate_cost():
    db = Database()
    records = db.read_user_data()
    data = []

    for row in records[:3]:
        accountid = row["accountid"]
        country = row["country"]
        city = row["city"]
        address = row["address"]

        full_address = "country="+country+", city="+city+", address="+address

        neighborhood_prompt = full_address+"\nGive me neighborhood address from given address and provide response in {'address': neighborhood_address} format only. If neighborhood_address not found than {'address': '', 'error': error}"

        query = AskQuery(neighborhood_prompt)
        response = query.get_response()
        print('response: ', response)
        if type(response) != "json":
            response = json.loads(response.replace("'", "\""))
        
        neighborhood_address = response["address"]
        if len(neighborhood_address) > 10:
            full_address = neighborhood_address

        print('full_address: ', full_address)
        cost_prompt = full_address+"\nGive me accurate cost in dollar per meter square for given address and provide response in {'cost': actual_cost} format only. If actual_cost not found than {'cost': 0, 'error': error}"

        query = AskQuery(cost_prompt)
        response = query.get_response()
        if type(response) != "json":
            response = json.loads(response.replace("'", "\""))
        print('response_json: ', response["cost"])
        cost = response["cost"]

        query = "INSERT INTO Employees (accountid, address, cost_per_square_meter ) VALUES (%s, %s, %s)"
        data.append((accountid, full_address, cost))

    # db.insert_data(query, data)


calculate_cost()
