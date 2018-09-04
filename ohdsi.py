"""
Methods to handle exporting results to OHDSI DB
"""
from pymongo import MongoClient
import util

class ExportOhdsi:

    def exportResults(self):
        client = MongoClient(util.mongo_host, util.mongo_port)
        db = client[util.mongo_db]
        collection = db['phenotype_results']
        cursor = collection.find({})

        for entry in cursor:
            print (entry)
