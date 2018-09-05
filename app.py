from flask import Flask, request, jsonify, response
import util
import ohdsi

app = Flask(__name__)

@app.route("/")
def hello():
    return "Welcome to ClarityNLP Export Results Module"


@app.route("/export_ohdsi", methods=['POST'])
def export_ohdsi():
    if request.method == 'POST':
        r = request.get_json()
        e = ohdsi.ExportOhdsi()
        response = e.exportResults(r['job_id'], r['result_name'], r['omop_domain'], r['concept_id'])
        #response = e.exportResults(10000,"Temperature",'Measurement',3020891)
        return response
    else:
        return Response('{"message":"This API supports only POST requests"}', status=400, mimetype='application/json')




if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
