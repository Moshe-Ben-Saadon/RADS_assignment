from pymongo import MongoClient
from random import randint

# Connect to MongoDB
client = MongoClient("mongodb+srv://MBS:12345678MBS@cluster0.7fqbr.mongodb.net/Cluster0?retryWrites=true&w=majority")
db = client.advertisements

# Create randomized data
names = ['Kitchen', 'Animal', 'State', 'Tastey', 'Big', 'City', 'Fish', 'Pizza', 'Goat', 'Salty', 'Sandwich', 'Lazy',
         'Fun']
# company_type = ['LLC', 'Inc', 'Company', 'Corporation']
company_cuisine = ['Pizza', 'Bar Food', 'Fast Food', 'Italian', 'Mexican', 'American', 'Sushi Bar', 'Vegetarian']

for x in range(1, 20001):
    ads = {
        'index': x,
        'data': names[randint(0, (len(names) - 1))] + ' ' + names[randint(0, (len(names) - 1))] + ' ' + company_cuisine
        [randint(0, (len(company_cuisine) - 1))]
    }

    # Insert ads object directly into MongoDB via insert_one
    result = db.records.insert_one(ads)

    # Print to the console the ObjectID of the new document
    print('Created {0} of 20000 as {1}'.format(x, result.inserted_id))

# Announce upon completion
print('finished creating 20000 business reviews')

