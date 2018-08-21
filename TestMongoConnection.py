import json
import sys
from datetime import datetime

from MongoConnection import MongoConnection

# Establish a connection to the MongoDB Instance for city.inspections
mongo = MongoConnection(db = 'market', collection = 'stocks')

# returns the JSON value of a file
def read_doc_file(file):    
    docs = None
    with open(file, 'r') as f:
        docs = json.loads(f.read())

    return docs

# Updates the volume of a document found by Ticker
def update_volume(ticker, volume):
    query = {"Ticker" : ticker}
    update = {"$set": {"Volume" : volume}}

    return mongo.update_documents(query, update)

# Performs a delete many for a given Ticker value
def delete_ticker(ticker):
    query = {"Ticker" : ticker}

    return mongo.delete_documents(query)

# Counts documents of a key between a range
def find_low_high_count(key, low, high):
    query = {key: {"$gte" : low, "$lte" : high}}

    return mongo.count_documents(query)

# Return documents for a specific key and value
def find_tickers_by_key_value(key, value):
    query = {key: value}
    projection = {"_id" : 0, "Ticker" : 1}

    return mongo.find_documents(query, projection)   

# Performs an aggregation Totalling Outstanding Shares for a given Sector
def aggregate_sector(sector):
    pipeline = [
        {"$match" : {"Sector" : sector}},
        {"$group" : {
            "_id" : "$Industry", 
            "Total Shares Outstanding" : {"$sum" : "$Shares Outstanding"}
            }
        }
    ]

    return mongo.aggregate_documents(pipeline)