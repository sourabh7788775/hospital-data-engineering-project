from pymongo import MongoClient
import pandas as pd

MONGO_URI = "mongodb+srv://sourabh5423043_db_user:hvslTR1n57gBTVJg@hospitalcluster.8uj8iyp.mongodb.net/?retryWrites=true&w=majority&appName=HospitalCluster"

client = MongoClient(MONGO_URI)

db = client["hospital_db"]
collection = db["patients"]

# Optional: Purana data delete kar dega
collection.delete_many({})

df = pd.read_csv("hospital_dataset_10000.csv")

records = df.to_dict(orient="records")

collection.insert_many(records)

print("===================================")
print("10000 Records Uploaded Successfully")
print("===================================")
print("Total Records :", collection.count_documents({}))