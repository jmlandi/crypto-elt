from src.controller import Controller
from flask import Flask, request
app = Flask(__name__)

@app.route('/')
def root():
    return Controller.root()

@app.route('/data/refresh', methods=['GET'])
def refresh_data():
    return Controller.refresh_data()

@app.route('/data/query', methods=['POST'])
def query_data(request=request):
    return Controller.query_data(request)

if __name__ == "__main__":
    Controller.refresh_data()  # Initial data load
    app.run(host='0.0.0.0', port=3000)