import pymongo
from datetime import datetime
from RADS_assignment import Functions as fcn

# Connect to MongoDB
client = pymongo.MongoClient("mongodb+srv://"
                             "MBS:12345678MBS@cluster0.7fqbr.mongodb.net/Cluster0?retryWrites=true&w=majority")

start_time = datetime.now()
data_chunks = fcn.data_retrieval(2000, 20000, client.advertisements.records)
sorted_database = fcn.k_way_merge_sort(data_chunks)

# Measure the duration of the process
elapsed_time = datetime.now() - start_time
print(elapsed_time)
# # Store the time in the aforementioned table, in an appropriate column #TODO fix this
# client.RESULTS.insert_one({'Sorting_step2_Process_time': elapsed_time.seconds})



