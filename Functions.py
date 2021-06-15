import pymongo


def data_retrieval(size, database):
    # Connect to MongoDB
    client = pymongo.MongoClient("mongodb+srv://"
                                 "MBS:12345678MBS@cluster0.7fqbr.mongodb.net/Cluster0?retryWrites=true&w=majority")
    # Retrieve database
    db = client["advertisements"]

    counter = 0
    data = list()
    list_of_chunks = list()
    for x in database.find().sort("data", pymongo.ASCENDING):
        data.append(x["data"])
        counter += 1
        if counter == (size - 1):
            list_of_chunks.append(data)
            data = []
            counter = 0
    return list_of_chunks


