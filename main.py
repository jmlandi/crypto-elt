from src.controller import Controller
from src.initializer import Initializer
from flask import Flask, request

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
    Initializer.setup(app)