# simple-app.py

from flask import Flask, jsonify
import json
from flask_cors import CORS
from subprocess import Popen

app = Flask(__name__)


@app.route('/', methods=['GET'])
def root():

    print("Executing Root...")
        
    return jsonify({
        "message": "Api Executing OK", 
        "status": "Pass"})


@app.route('/execute-json', methods=['GET'])
def execute_json():

    print("Executing Command...")
    
    cmd = 'spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:2.4.5,io.delta:delta-core_2.12:0.7.0 --master local[*] --driver-memory 4g --executor-memory 4g /home/jovyan/work/app/json-producer.py'
    
    p = Popen(['watch', cmd]) # something long running
    
    #p.terminate()

    return jsonify({
        "message": "Command JSON Executed OK", 
        "status": "Pass"})

if __name__ == "__main__":
    app.run(debug=True, port = 5000, host='0.0.0.0')