import os
import openai
import json
from database import Database

from dotenv import load_dotenv

load_dotenv("/home/ec2-user/analyze-address-cost/dot.env")

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

    for row in records:
        data = []
        accountid = row["accountid"]
        country = row["country"]
        city = row["city"]
        address = row["address"]

        full_address = "country="+country+", city="+city+", address="+address
        address = country+", "+city+", "+address

        neighborhood_prompt = full_address+"\nGive me neighborhood address from given address and provide response in {'address': neighborhood_address} format only. If neighborhood_address not found than {'address': '', 'error': error}. \nReturn the JSON formatted with {} and don't wrap with ```json.\nneighborhood_address should not contains single quote and apostrophe s. neighborhood_address must be in a string."

        query = AskQuery(neighborhood_prompt)
        response = query.get_response()
        print("response: ", response)
        if type(response) != "json":
            try:
                response = json.loads(response.replace("'", "\""))
            except:
                response = {"address": ""}
        
        neighborhood_address = response["address"]
        if len(neighborhood_address) > 10:
            cost_prompt = neighborhood_address+"\nGive me accurate cost in dollar per meter square for given address and provide response in {'cost': actual_cost} format only. If actual_cost not found than {'cost': 0, 'error': error}. Return the JSON formatted with {} and don't wrap with ```json."

            query = AskQuery(cost_prompt)
            response = query.get_response()
            if type(response) != "json":
                try:
                    response = json.loads(response.replace("'", "\""))
                except:
                    response = {"cost": ""}
            print('Cost 1: ', response["cost"])
            cost = response["cost"]

            data.append((accountid, address, cost))

        cost_prompt = full_address+"\nGive me accurate cost in dollar per meter square for given address and provide response in {'cost': actual_cost} format only. If actual_cost not found than {'cost': 0, 'error': error}. Return the JSON formatted with {} and don't wrap with ```json."
        query = AskQuery(cost_prompt)
        response = query.get_response()
        print("response original add: ", response)
        if type(response) != "json":
            try:
                response = json.loads(response.replace("'", "\""))
            except:
                response = {"address": ""}
        print('Cost 2: ', response["cost"])
        cost = response["cost"]

        data.append((accountid, address, cost))
        query = "INSERT INTO [dbo].[client_location_cost] (accountid, client_neighborhood, cost_per_sqm) VALUES (%s, %s, %s)"

        db.insert_data(query, data)
    db.read_cost_data()


calculate_cost()
