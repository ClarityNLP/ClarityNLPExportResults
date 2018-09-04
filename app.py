from flask import Flask
import util

app = Flask(__name__)

@app.route("/")
def hello():
    return "Welcome to ClarityNLP Export Results Module"



if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
