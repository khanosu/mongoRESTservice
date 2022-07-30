from flask import Flask, jsonify, request
from jinja2 import pass_environment
from pymongo import MongoClient
from bson import ObjectId
from flask_httpauth import HTTPBasicAuth

app = Flask(__name__)
client = MongoClient("mongodb://localhost:27017")

auth = HTTPBasicAuth()

config_db = client["REST_config"]
config_collection = config_db["REST_collection"]

########## authenticate #########
# this method is called automatically for autthentication
@auth.verify_password
def authenticate(username, password):

    auth_db = client["funel_api_account"]
    auth_collection = auth_db["users"]

    auth_data = auth_collection.find_one()
    auth_user = auth_data["user"]
    auth_password = auth_data["password"]

    if (username and password):
        if (username == auth_user and password == auth_password):
            return True
    else:
        return False
    return False

########## post_config #########
#
#  Interface:
#  Expects client to send data as,
#  {'db_name': db_name, 'collection_name': col_name, 'date_time': datetime}
#
#  We need to persist this info between calls to api since invocations of RESTful api meethods are 
#  stateless
#

@app.route('/mongo/config', methods=['POST'])
@auth.login_required
def post_config():
    data_from_client = request.get_json(force=True) 
    config_data = {"db": data_from_client["db_name"], "collection": data_from_client["collection_name"], "date_time": data_from_client['date_time']}

    config_collection.delete_many({})
    config_collection.insert_one(config_data)

    del config_data['_id']
    return jsonify(config_data)

########## show_dbs #########

@app.route('/mongo/show_dbs', methods=['GET'])
@auth.login_required
def show_dbs():

    db_list=[]
    for db in client.list_databases():
       db_list.append(db)

    return jsonify(db_list)

########## show_collections #########

@app.route('/mongo/show_collections', methods=['GET'])
@auth.login_required
def show_collections():
    args = request.args
    db_name = args["db_name"]
    db=client[db_name]

    collection_list=[]
    for collection in db.list_collection_names():
       collection_list.append(collection)

    return jsonify(collection_list)

########## find many #########

@app.route('/mongo', methods=['GET'])
@auth.login_required
def find_many():
    args = request.args
    docs_limit = int(args["limit"])

    config_data = config_collection.find_one()
    db_name = config_data["db"]
    collection_name = config_data["collection"]

    db=client[db_name]
    collection=db[collection_name]

    docs_list = []
    docs = collection.find().sort("_id", -1).limit(docs_limit)
    # docs is a cursor
    # use docs for iteration 
    for doc in docs:
        #del doc['_id']
        doc['_id'] = str(doc['_id'])
        docs_list.append(doc)
    return jsonify(docs_list)

########## find by id #########

@app.route('/mongo/get_by_id', methods=['GET'])
@auth.login_required
def find_by_id():
    args = request.args
    id_string = args["id_string"]

    config_data = config_collection.find_one()
    db_name = config_data["db"]
    collection_name = config_data["collection"]

    db=client[db_name]
    collection=db[collection_name]

    doc = collection.find_one({"_id" : ObjectId(id_string)})
    doc['_id'] = str(doc['_id'])
    return jsonify(doc)
    
########## insert_one #########

@app.route('/mongo', methods=['POST'])
@auth.login_required
def insert_one():
    data_from_client = request.get_json(force=True) 

    config_data = config_collection.find_one()
    db_name = config_data["db"]
    collection_name = config_data["collection"]

    db=client[db_name]
    collection=db[collection_name]

    collection.insert_one(data_from_client)
    return jsonify({"insert_one": "OK"})

########## insert_many  #########

@app.route('/mongo/insert_many', methods=['POST'])
@auth.login_required
def insert_many():
    data_from_client = request.get_json(force=True) 

    config_data = config_collection.find_one()
    db_name = config_data["db"]
    collection_name = config_data["collection"]

    db=client[db_name]
    collection=db[collection_name]

    collection.insert_many(data_from_client)
    return jsonify({"insert_many": "OK"})

########## delete_by_id #########

@app.route('/mongo', methods=['DELETE'])
@auth.login_required
def delete_by_id():
    args = request.args
    id_string = args["id_string"]

    config_data = config_collection.find_one()
    db_name = config_data["db"]
    collection_name = config_data["collection"]

    db=client[db_name]
    collection=db[collection_name]

    result = collection.delete_one({"_id" : ObjectId(id_string)})
    return jsonify({"delete by _id": "OK"})

########## delete_all #########

@app.route('/mongo/delete-all', methods=['DELETE'])
@auth.login_required
def delete_all():

    config_data = config_collection.find_one()
    db_name = config_data["db"]
    collection_name = config_data["collection"]

    db=client[db_name]
    collection=db[collection_name]

    collection.delete_many({})
    
    return jsonify({"delete all": "OK"})

########## delete_one #########

@app.route('/mongo/delete-one', methods=['DELETE'])
@auth.login_required
def delete_one():

    config_data = config_collection.find_one()
    db_name = config_data["db"]
    collection_name = config_data["collection"]

    db=client[db_name]
    collection=db[collection_name]

    collection.delete_one({})
    
    return jsonify({"delete one": "OK"})


if __name__ == '__main__':
    #app.run(host='0.0.0.0', port=5000, debug=True)
    app.run(host='0.0.0.0', port=5000)
    #app.run()
