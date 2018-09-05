"""
Methods to handle exporting results to OHDSI DB
"""
import json
import string
import psycopg2
import psycopg2.extras
import datetime
import util
from pymongo import MongoClient
import pymysql
from datetime import datetime


"""
Class to handle exporting results to OHDSI database
"""
class ExportOhdsi:
    """
    Function for exporting results
    """
    def exportResults(self, job_id, result_name, omop_domain, concept_id):
        # Grabbing results from Mongo
        results = self.filterResults(job_id, result_name)

        # No results matching criteria
        if results.count() == 0:
            return JSON.dumps({"status":200, "message":"No results matching the given job id and result name."})

        # Write results to the OMOP
        responseMsg = self.writeResults(results, omop_domain, concept_id)


        pass

    """
    Function to read the results based on input parameters from Mongo
    """
    def filterResults(self, job_id, result_name):
        client = MongoClient(util.mongo_host, util.mongo_port)
        db = client[util.mongo_db]
        collection = db['phenotype_results']
        cursor = collection.find({'job_id':job_id, 'nlpql_feature':result_name})
        return cursor

    """
    Function to write results to the OMOP database
    """
    def writeResults(self, results, omop_domain, concept_id):
        # Connecting to OMOP database
        conn = self.connectToOMOP()
        if conn is None:
            return JSON.dumps({"status":404, "message":"Can not connect to OMOP database"})
        cursor = conn.cursor()

        # Getting the primary key for the new entries
        primary_key = self.getPrimaryKey(cursor, omop_domain)
        print ("Primary Key = " + str(primary_key))
        if primary_key is None:
            return JSON.dumps({"status":500, "message":"Can't read data from OMOP database"})

        # Writing results to the database
        response = self.write2DB(cursor, omop_domain, concept_id, results, primary_key)

        # Committing changes to DB
        conn.commit()
        conn.close()
        return

        pass

    """
    Function to connect to the OMOP Database
    """
    def connectToOMOP(self):
        db_type = util.ohdsi_db_type
        if db_type == '1':
            conn = psycopg2.connect(util.ohdsi_conn_string)
        elif db_type == '2':
            conn = pymysql.connect(util.ohdsi_conn_string)
        else:
            return None

        return conn

    """
    Function to get the primary key for results to be exported
    """
    def getPrimaryKey(self, cursor, omop_domain):
        if omop_domain == 'Condition':
            cursor.execute("SELECT MAX(condition_occurrence_id) FROM " + util.ohdsi_schema + ".condition_occurrence;")
            return cursor.fetchall()[0][0] + 1
        if omop_domain == 'Measurement':
            cursor.execute("SELECT MAX(measurement_id) FROM " + util.ohdsi_schema + ".measurement;")
            return cursor.fetchall()[0][0] + 1

    """
    Function to write results to database
    """
    def write2DB(self, cursor, omop_domain, concept_id, results, primary_key):
        # Map: omop_domain -> concept_id
        domain_map = {
            'Condition': 25000000,
            'Measurement': 25000004,
            'Observation':25000002
        }

        for entry in results:
            if omop_domain == 'Condition':
                report_date = datetime.strptime(entry['report_date'][0:entry['report_date'].find('T')], '%Y-%m-%d')
                source_value = "Job=" + str(entry['job_id']) + "|Pipeline=" + str(entry['pipeline_id'])
                query = "INSERT INTO " + util.ohdsi_schema + ".condition_occurrence(condition_occurrence_id, person_id, condition_concept_id, condition_start_date, condition_type_concept_id, condition_source_value) VALUES (%s, %s, %s, %s, %s, %s)"
                cursor.execute(query, (primary_key, entry['subject'], concept_id, report_date, domain_map[omop_domain], source_value))
            elif omop_domain == 'Measurement':
                report_date = datetime.strptime(entry['report_date'][0:entry['report_date'].find('T')], '%Y-%m-%d')
                query = "INSERT INTO " + util.ohdsi_schema + ".measurement(measurement_id, person_id, measurement_concept_id, measurement_date, measurement_type_concept_id, value_as_number) VALUES (%s, %s, %s, %s, %s, %s)"
                cursor.execute(query, (primary_key, entry['subject'], concept_id, report_date, domain_map[omop_domain], entry['value']))
            break

        return ""
