from src.controller import Controller
from flask import Flask, request
import os
app = Flask(__name__)

@app.route('/')
def root():
    return Controller.root()

@app.route('/data/refresh', methods=['GET'])
def refresh_data():
    return Controller.refresh_data()

@app.route('/data/query', methods=['POST'])
def query_data():
    data = request.get_json()
    query = data.get('query')
    dataset = data.get('dataset', 'raw')
    return Controller.query_data(query, dataset)

if __name__ == "__main__":
    os.makedirs('./data/raw', exist_ok=True)
    os.makedirs('./data/analytics', exist_ok=True)
    Controller.refresh_data()  # Initial data load
    app.run(host='0.0.0.0', port=3000)