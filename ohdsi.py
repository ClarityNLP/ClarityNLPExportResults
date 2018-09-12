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
from flask import Response


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
            return Response('No results matching the given job id and result name.', status=400, mimetype='application/json')

        # Write results to the OMOP
        response = self.writeResults(results, omop_domain, concept_id)

        return response

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
            return Response('Can not connect to OMOP database', status=404, mimetype='application/json')
        cursor = conn.cursor()

        # Getting the primary key for the new entries
        primary_key = self.getPrimaryKey(cursor, omop_domain)
        print ("Primary Key = " + str(primary_key))
        if primary_key is None:
            return Response('Cant read data from OMOP database', status=500, mimetype='application/json')

        # Writing results to the database
        self.write2DB(cursor, omop_domain, concept_id, results, primary_key)

        # Committing changes to DB
        try:
            conn.commit()
        except:
            return Response('Cant write data to OMOP database', status=500, mimetype='application/json')
            self.deleteExportedResults(cursor, conn, primary_key, omop_domain)

        conn.close()

        return Response('Successfully exported results', status=200, mimetype='application/json')



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
                try:
                    cursor.execute(query, (primary_key, entry['subject'], concept_id, report_date, domain_map[omop_domain], source_value))
                except:
                    continue

            elif omop_domain == 'Measurement':
                report_date = datetime.strptime(entry['report_date'][0:entry['report_date'].find('T')], '%Y-%m-%d')
                query = "INSERT INTO " + util.ohdsi_schema + ".measurement(measurement_id, person_id, measurement_concept_id, measurement_date, measurement_type_concept_id, value_as_number) VALUES (%s, %s, %s, %s, %s, %s)"
                try:
                    cursor.execute(query, (primary_key, entry['subject'], concept_id, report_date, domain_map[omop_domain], entry['value']))
                except:
                    continue

            elif omop_domain == 'Observation':
                report_date = datetime.strptime(entry['report_date'][0:entry['report_date'].find('T')], '%Y-%m-%d')
                query = "INSERT INTO " + util.ohdsi_schema + ".observation(observation_id, person_id, observation_concept_id, observation_date, observation_type_concept_id, value_as_number) VALUES (%s, %s, %s, %s, %s, %s)"
                try:
                    cursor.execute(query, (primary_key, entry['subject'], concept_id, report_date, domain_map[omop_domain], entry['value']))
                except:
                    continue

            break # TODO: remove this break

    """
    Function to delete recently added results
    """
    def deleteExportedResults(self, cursor, conn, primary_key, omop_domain):
        if omop_domain == 'Condition':
            query = "DELETE FROM " + util.ohdsi_schema + ".condition_occurrence WHERE condition_occurrence_id=%s"
        elif omop_domain == 'Measurement':
            query = "DELETE FROM " + util.ohdsi_schema + ".measurement WHERE measurement_id=%s"
        elif omop_domain == 'Observation':
            query = "DELETE FROM " + util.ohdsi_schema + ".observation WHERE observation_id=%s"

        cursor.execute(query, (primary_key, ))
        conn.commit()


        cursor.execute()
