import pymongo
from datetime import datetime

# Connect to MongoDB
client = pymongo.MongoClient("mongodb+srv://"
                             "MBS:12345678MBS@cluster0.7fqbr.mongodb.net/Cluster0?retryWrites=true&w=majority")
# Retrieve database
db = client["advertisements"]

# Retrieve collection
ads_collection = db["records"]

# Retrieve document and query the data to a local sorted list
data = list()
start_time = datetime.now()
for x in ads_collection.find().sort("data", pymongo.ASCENDING):
    data.append(x["data"])

# Store the sorted data in a new table under a specific column
sorted_records = {
    'Sorting-step1': data
}
db.RESULTS.insert_one(sorted_records)

# Measure the duration of the process
elapsed_time = datetime.now() - start_time

# Store the time in the aforementioned table, in an appropriate column
db.RESULTS.insert_one({'Sorting_step1_Process_time': elapsed_time.seconds})

print("debug here")

# Initialize an empty table for results
# RESULTS = pd.DataFrame(data, columns=['Sorting-step1'])