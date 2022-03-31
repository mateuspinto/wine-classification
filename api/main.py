from flask import Flask, request

import joblib

APP = Flask(__name__)


@APP.route('/', methods=['GET'])
def info():
    return {'error': 0, 'pipe': str(PRICE_MODEL)}


@APP.route('/predict/price', methods=['POST'])
def predict_wine_price():
    WINE_INFO = [0, 0, int(request.form['is_red']), float(request.form['fixed_acidity']), float(request.form['volatile_acidity']), float(request.form['citric_acid']), float(request.form['residual_sugar']), float(request.form['chlorides']), float(request.form['free_sulfur_dioxide']), float(request.form['total_sulfur_dioxide']), float(request.form['density']), float(request.form['ph']), float(request.form['sulphates']), float(request.form['alcohol']), 0, 0]
    PREDICTION = PRICE_MODEL.predict([WINE_INFO])[0]
    return {'error': 0, 'predict': round(PREDICTION, 2)}


if __name__ == '__main__':
    PRICE_MODEL = joblib.load('models/price_model.joblib')
    APP.run(port=8080)
