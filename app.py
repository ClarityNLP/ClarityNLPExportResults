from flask import Flask
import util
import ohdsi

app = Flask(__name__)

@app.route("/")
def hello():
    return "Welcome to ClarityNLP Export Results Module"


@app.route("/export_ohdsi")
def export_ohdsi():
    e = ohdsi.ExportOhdsi()
    e.exportResults()
    return ""




if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
