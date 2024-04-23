# Technical-Test

This is my Practical Test for Data Engineer Position
Python program is in a jupyter notebook provided with code & results

# Requirement
- Python 3
  - pymongo (installed with pip)
  - jupyter notebook (installed with pip)
- MongoDB running on localhost without password and port 27017 (default)

# Architectural diagramme 
![Diagramme.svg](Diagramme.svg)

# Technical choice :
## Data ingestion :
I assumed that files are present on a local file system (with node for example)

Automated ingestion of a CSV : Python (requirement)
## Data transformation :
Data cleaning & validation & cleaning :  Python (requirement)
## Data Storage :
Must support operational activities and analytical queries with high availability and can be efficiently accessed by a web application for both reads and writes.

    • Apache Cassandra 
    • AWS Aurora
    • AWS Redshift

Could be good for this, but I’ll chose **MongoDB** because It’s easy to setup and that’s a good fit for a practical test. MongoDB provide built in access from various connectors like pymongo and an an 
interface should look like that example : 
```
from pymongo import MongoClient
from datetime import datetime

class CloudMongoDBInterface:
    def __init__(self, connection_string, database_name):
        self.client = MongoClient(connection_string)
        self.db = self.client[database_name]

    def store_data(self, collection_name, data):
        collection = self.db[collection_name]
        result = collection.insert_one(data)
        return result.inserted_id

    def retrieve_data(self, collection_name, query):
        collection = self.db[collection_name]
        return collection.find(query)

    def update_data(self, collection_name, query, new_values):
        collection = self.db[collection_name]
        result = collection.update_many(query, {"$set": new_values})
        return result.modified_count

    def delete_data(self, collection_name, query):
        collection = self.db[collection_name]
        result = collection.delete_many(query)
        return result.deleted_count

# Example usage
if __name__ == "__main__":
    # Initialize the interface
    interface = CloudMongoDBInterface(connection_string='mongodb://localhost:27017/', database_name='example_db')

    # Example data to store
    example_data = {
        "name": "John",
        "age": 30,
        "email": "john@example.com",
        "created_at": datetime.now()
    }

    # Store data
    inserted_id = interface.store_data(collection_name='users', data=example_data)
    print("Data inserted with ID:", inserted_id)

    # Retrieve data
    retrieved_data = interface.retrieve_data(collection_name='users', query={"name": "John"})
    for data in retrieved_data:
        print("Retrieved data:", data)

    # Update data
    update_query = {"name": "John"}
    new_values = {"$set": {"age": 31}}
    modified_count = interface.update_data(collection_name='users', query=update_query, new_values=new_values)
    print("Modified count:", modified_count)

    # Delete data
    delete_query = {"name": "John"}
    deleted_count = interface.delete_data(collection_name='users', query=delete_query)
    print("Deleted count:", deleted_count)
```

# Data Analysis and Reporting
AWS provide a good service of analysis and reporting with OpenSearch 
but since were not on a cloud platform I'll choose Apache Superset to create a Dashboard.
Apache superset provide an easy to use connector to mongoDB

MongoDB provide and easy web Acces, so we can create a basic application in desired framework (Angular, React etc...)

# Note Concerning Tests & Data Quality

MongoDB provide a test on _id to be unique. For Silver or Gold grade we can enshure that an id from a collection match on an other collection.
Catching errors provide a way of keeping duplicated or malformed reccord into an other table.

Not test on the code it self is provided 
