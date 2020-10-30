from pprint import pprint
from typing import Optional
import boto3, json
from boto3.dynamodb.conditions import Key
from fastapi import FastAPI
from pydantic import BaseModel

client = boto3.client('dynamodb',aws_access_key_id='yyyy', aws_secret_access_key='xxxx', region_name='***')

@app.get("/")
async def root():
    return {"message": "Hello World"}
    
@app.get("/{tablename}/{primary_key}/{primary_key_value}")
def read_item(tablename: str, primary_key: str, primary_key_value: str):
    database = get_database()
    pprint(f"Database {database}")
    item = get_item(database, primary_key, primary_key_value, tablename)
    pprint(f"Item: {item}")
    message = f"Endpoint: {tablename}\n Database: {database}"
    return {'connection_details': message, 'item_queried': item}

def get_item(database, primary_key, primary_key_value, tablename: str):
    table_id = database.Table(tablename)
    result = table_id.get_item(Key = {primary_key: primary_key_value})
    if not result:
        return None
    item = result.get('Item')
    return item

def getDatabase(Exp, dynamodb=None):
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb')

    table = dynamodb.Table('plantDDB4rmS3')
    response = table.query(
        KeyConditionExpression=Key('Exp').eq(Exp)
    )
    return response['Items']

def get_experiment_row(queryExp: str):
    result = getDatabase(queryExp)
    if not result.value:
        return None
    return result

if __name__ == '__main__':
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('plantDDB4rmS3')   
    client = boto3.client('dynamodb')
    app = FastAPI() # FastAPI specific code
    queryExp = "9C7-1"
    get_experiment_row(queryExp)
    exps = getDatabase(queryExp)
    for exp in exps:
        print(exp)
        print(exp['Exp'], ":", exp['A_1'], ":", exp['A_2'], ":", exp['A_3'], ":", exp['A_4'], ":", exp['A_5'], ":", exp['B_1'], ":", exp['B_2'], ":", exp['B_3'], ":", exp['B_4'], ":", exp['B_5'], ":", exp['C_1'], ":", exp['C_2'], ":", exp['C_3'], ":", exp['C_4'], ":", exp['C_5'], ":", exp['L_1'], ":", exp['L_10'], ":", exp['L_2'], ":", exp['L_3'])