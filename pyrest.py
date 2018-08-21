import json
import os
import pprint
import sys
from datetime import datetime

from bson import json_util

# PyMongo
from pymongo import MongoClient

# Bottle imports
#import bottle
from bottle import abort, delete, get, post, put, request, route, run, template

# Seperate class file for handling mongo requests
from MongoConnection import MongoConnection

'''GLOBALS'''
# Connect to MongoDB for the provided DB and Collection
mongo = MongoConnection(db = 'market', collection = 'stocks')

# Pretty Printer
pp = pprint.PrettyPrinter(indent=2)

@post('/createStock/<ticker>')
def post_createStock(ticker=None):
    '''
    HTTP POST method to create a new stock.

    Requires <ticker> string variable in URI and valid JSON data.

    Returns result in JSON format.
    '''

    if ticker:
        doc = {"Ticker" : ticker}

        if request.json:            
            doc.update(request.json) # Merge ticker and JSON data together

            # Insert document and retrieve result
            result = mongo.insert_document(doc)

            string = template("{ \"result\" : \"{{result}}\" }", result=result.acknowledged)
        else:
            abort(404, "Invalid arguments. Expected JSON data.")
    else:
        abort(404, "Invalid arguments. Expected ticker name.")

    return json.loads(json.dumps(string, indent=4, default=json_util.default))

@get('/getStock/<ticker>')
def get_stock(ticker=None):
    '''
    HTTP GET method to retrieve document from MongoDB based on Ticker field.

    Requires <ticker> string variable in the URI.

    Returns string extraction of JSON data.
    '''

    if ticker:
        doc = mongo.find_document({"Ticker" : ticker})

        if not doc:
            abort(404, "No stock found with ticker name: {}".format(ticker))
    else:
        abort(404, "Invalid arguments. Expected ticker name. No ticker argument provided.")

    return json.dumps(doc, indent=4, default=json_util.default)

@put('/updateStock/<ticker>')
def update_stock(ticker):
    '''
    HTTP PUT method to update a document in MongoDB based on Ticker field.

    Requires <ticker> string variable in URI and valid JSON data.

    Returns result in JSON format.
    '''

    if ticker:
        doc = {"Ticker" : ticker}

        if request.json:
            update = {"$set" : request.json}

            result = mongo.update_document(doc, update)
        else:
            abort(404, "Invalid arguments. Expected JSON data.")    
    else:
        abort(404, "Invalid arguments. Expected ticker name. No ticker argument provided.")

    return result.raw_result

@delete('/deleteStock/<ticker>')
def delete_stock(ticker=None):
    '''
    HTTP DELETE method to delete a document in MongoDB based on Ticker field.

    Requires <ticker> string value in URI.

    Returns result in JSON format.
    '''
    if ticker:  
        doc = {"Ticker" : ticker}

        result = mongo.delete_document(doc)    
    else:
        abort(404, "Invalid arguments. Expected ticker name. No ticker argument provided.")

    return result.raw_result

@get('/stockReport/<stocks>')
def get_stockReport(stocks=None):
    '''
    HTTP GET method to retrieve a customized summary report for list of stocks.

    Requires <stocks> string value in URI.
    <stocks> must be in square bracket, comma seperated notation:
        [AAPL,GOOG,AMZN,MSFT]

    Returns string extraction of JSON data.    
    '''
    if stocks: 
        if "[" in stocks and "]" in stocks:
            stocks = stocks.replace("[", "").replace("]","")
            stocks = stocks.split(",")
        else:
            abort(404, "Invalid stocks list format. Expected comma seperated list in square brackets.")    
     
        pipeline = [
            {"$match" : {"Ticker" : {"$in" : stocks}}},
            {"$project" : { 
                "_id" : 0,  
                "Ticker" : 1,  
                "Price" : 1,
                "Change" : 1,  
                "Volume" : 1,  
                "Market Cap" : 1, 
                "Change %" : {"$multiply" : [ {"$divide" : [ "$Change", {"$subtract" : ["$Price", "$Change"]} ] }, 100 ] }
                }
            }
        ]

        doc = mongo.aggregate_documents(pipeline)

        if not doc:
            abort(404, "No documents returned for Ticker list: {}".format(stocks))
    else:
        abort(404, "Invalid stocks list format. Expected comma seperated list in square brackets.")

    return json.dumps(doc, indent=4, default=json_util.default)

@get('/industryReport/<industry>')
def industry_report(industry=None):
    '''
    HTTP GET method to retrieve a customized industry report for Industry keyword.
    Performs a Full-Text Search for the Industry keyword provided.

    Requires <industry> string value in URI.
    <industry> can be any valid string value, including spaces:
        telecom
        Meat Products

    Returns string extraction of JSON data.    
    '''
    if industry:
        pipeline = [
             {"$match" : {"$text" : {"$search" : industry}}},
             {"$project" : {"_id" : 0, "Ticker" : 1, "Price" : 1}}, 
             {"$sort" : {"Price" : -1}}, 
             {"$limit" : 5}
        ]

        # Retrieve data from MongoDB for provided pipeline
        doc = mongo.aggregate_documents(pipeline)

        if not doc:
            abort(404, "No industry found by keyword: {}".format(industry))

    else:
        abort(404, "Invalid arguments. Expected industry.")

    return json.dumps(doc, indent=4, default=json_util.default)

@get('/portfolio/<company>')
def portfolio(company=None):
    '''
    HTTP GET method to retrieve top stocks in industry based on Company.

    Requires <company> string value in URI.
    <company>  can be any valid string value, including spaces:
        Amazon.com, Inc.

    Returns results from industry_report(industry)    
    '''

    if company:
        
        # Get document for provided Company
        doc = mongo.find_document({"Company" : company})
        
        if doc:
            # Extract industry value
            industry = doc.get("Industry")

        else:
            abort(404, "No company found with company name: {}".format(company))
    else:
        abort(404, "Invalid arguments. Expected company name.")

    # Return Industry Report (top 5) for selected Industry
    return industry_report(industry)

def test_mongo():
    import TestMongoConnection as test
    
    # Complete doc to insert into the database
    docs = test.read_doc_file('test_doc.json')

    # Create
    print("\n## Create a document ##")
    print("Inserting: {}".format(docs))
    insert_result = mongo.insert_documents(docs)
    print(insert_result.acknowledged)

    if insert_result.acknowledged is True:       
        # Print results of inserted documents
        for id in insert_result.inserted_ids:
            try:
                # Set query to locate the inserted _id                
                query = {"_id" : id}
            except TypeError as e:
                sys.exit("Invalid query option.\nAn error of type {} has occured:\n{}".format(type(e).__name__, e))
            
            # Read
            print("\n## Read the documents ##")
            read_result = mongo.find_document(query)
            pp.pprint(read_result)        

        # Setup query
        query = {"Ticker" : "ZZZZ"}
        
        # Update  
        print("\n## Update the document ##")
        update_result = test.update_volume('ZZZZ', 9999)
        pp.pprint(update_result.raw_result)

        # Read (again)
        print("\n## Verify updated document ##")
        read_result = mongo.find_documents(query)
        pp.pprint(read_result)

        # Delete
        print("\n## Delete the document ##")
        delete_result = test.delete_ticker('ZZZZ')
        pp.pprint(delete_result.raw_result)

        # Read (one more time)
        print("\n## Verify deleted document ##")
        read_result = mongo.find_documents(query)
        pp.pprint(read_result)

        # Low-High Query
        print("\n## 50-Day Simple Moving Average ##")
        count = test.find_low_high_count("50-Day Simple Moving Average", 0.01, 0.02)
        pp.pprint(count)

        # Tickers by Key Value
        print("\n## Medical Laboratories & Research Tickers ##")
        tickers = test.find_tickers_by_key_value("Industry", "Medical Laboratories & Research")
        pp.pprint(tickers)

        # Aggregation Total Outstanding Shares
        print("\n## Aggretation Total Outstanding Shares by Sector ##")
        industries = test.aggregate_sector("Healthcare")
        pp.pprint(industries)

def show_help():
    """Displays the help from the command line"""
    filename = os.path.basename(__file__)
    print('Usage: {} [options]'.format(filename))	
    print("Options:")
    print("  -t,\t--testmode\tExecutes test suite for MongoConnection class.")	
    print('Examples:')
    print('  {}'.format(filename))
    print('  {} -t'.format(filename))
    print('  {} --testmode'.format(filename))

def main():
    # Run the bottle application
    run(host='0.0.0.0', port=8080)

if __name__ == "__main__":    
    if len(sys.argv) >= 2:
        if sys.argv[1].lower() in ("--testmode", "-t"):
                test_mongo()                
        else:
            show_help()
        sys.exit()   

    main()