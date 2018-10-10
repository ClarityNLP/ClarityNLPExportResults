from flask import Flask, request, jsonify, Response
from flask_cors import CORS
import util
import ohdsi

app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "*"}})

@app.route("/")
def hello():
    return "Welcome to ClarityNLP Export Results Module"


@app.route("/export_ohdsi", methods=['POST', 'GET'])
def export_ohdsi():
    if request.method == 'POST':
        r = request.get_json()
        print(r)
        if (r['job_id'] is None or len(r['result_name'])==0 or len(r['omop_domain'])==0 or len(r['concept_id'])==0):
            return Response('Bad Request. Missing fields.', status=400, mimetype='application/json')

        e = ohdsi.ExportOhdsi()
        response = e.exportResults(r['job_id'], r['result_name'], r['omop_domain'], r['concept_id'])

        #response = e.exportResults(10000,"Temperature",'Measurement',3020891)
        return response
    elif request.method == 'GET':
        return Response('API is healthy', status=200, mimetype='application/json')
    else:
        return Response('This API supports only POST & GET requests', status=400, mimetype='application/json')




if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
