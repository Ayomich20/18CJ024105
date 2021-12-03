from pymongo import MongoClient
import csv


def ingest_from_csv(filename, collection_name):
    try:
            with open(filename, newline='') as csvfile:
                reader = csv.reader(csvfile, delimiter=',')
                entry = {}
                for idx, row in enumerate(reader):
                    if idx == 0:
                        continue
                    try:
                        entry = {
                            "COUNTRY" : row[0],
                            "2010" : row[1],
                            "2011" : row[2],
                            "2012" : row[3],
                            "2013" : row[4],
                            "2014" : row[5],
                            "2015" : row[6],
                            "2016" : row[7],
                            "2017" : row[8],
                            "2018" : row[9],
                            "2019" : row[10],
                            
                            
                        }

                        collection_name.insert_one(entry)

                    except:
                        return

    except FileNotFoundError:
            print('cat.csv')

def get_database():
    CONNECTION_STRING = "mongodb://127.0.0.1:27017/?compressors=disabled&gssapiServiceName=mongodb"
    client = MongoClient(CONNECTION_STRING)
    return client['meat']

def print_collection(collection_name):   
    for item in collection_name.find():
        print(item)


if __name__ == "__main__":    
    dbname = get_database()
    collection_name = dbname['Meat_Stock']
    ingest_from_csv('cat.csv', item)
    print_collection(collection_name)
