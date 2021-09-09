# -*- coding: utf-8 -*-
"""
Created on Fri Sep  3 10:42:20 2021

@author: chunfang
"""

import json
from azureml.core import Model
import joblib
import pandas as pd

def init():
    global predictor, rel_cols, X_scaler, y_scaler
    # Get the path to the registered model file and load it
    model_path = Model.get_model_path('test')
    X_scalerpath = Model.get_model_path("X_scaler")
    predictor = joblib.load(model_path)
    X_scaler = joblib.load(X_scalerpath)
    all_columns = ['AINTEREST','COUPON FREQUENCY','EVAL MID PRICE','EVAL MID YIELD','MODIFIED DURATION','NEXT COUPON RATE','DAYS TO MATURITY',
               'CREDIT SPREAD','OPR MOVEMENT','RATING_AA','RATING_AA IS','RATING_AA+','RATING_AA+ IS','RATING_AA-','RATING_AA- IS',
               'RATING_AA- IS (CG)','RATING_AA1','RATING_AA1 (S)','RATING_AA2','RATING_AA2 (S)','RATING_AA3','RATING_AA3 (S)',
               'RATING_AAA','RATING_AAA (BG)','RATING_AAA (FG)','RATING_AAA (S)','RATING_AAA IS','RATING_AAA IS (FG)']
    schema = pd.DataFrame(columns=all_columns)
    rel_cols = schema.columns
    
def run(data):
    raw_data = json.loads(data)
    sample_value =  pd.DataFrame.from_dict(raw_data)
    output = pd.DataFrame()
    output['STOCK CODE'] = sample_value['STOCK CODE']
    sample_value = sample_value.drop(['STOCK CODE'],axis=1)
    output_dummy = pd.get_dummies(sample_value)
    missing_cols = rel_cols.difference(output_dummy.columns)
     
    for i in missing_cols:
        output_dummy[i] = 0
        
    output_dummy = output_dummy[['NEXT COUPON RATE','AINTEREST','COUPON FREQUENCY','EVAL MID PRICE',
                            'EVAL MID YIELD','MODIFIED DURATION','DAYS TO MATURITY','CREDIT SPREAD',
                            'OPR MOVEMENT','RATING_AA','RATING_AA IS','RATING_AA+','RATING_AA+ IS',
                            'RATING_AA-','RATING_AA- IS','RATING_AA- IS (CG)','RATING_AA1','RATING_AA1 (S)',
                            'RATING_AA2','RATING_AA2 (S)','RATING_AA3','RATING_AA3 (S)','RATING_AAA',
                            'RATING_AAA (BG)','RATING_AAA (FG)','RATING_AAA (S)','RATING_AAA IS',
                            'RATING_AAA IS (FG)']]
    scaled_data = X_scaler.transform(output_dummy)
    output_value = predictor.predict(scaled_data)
    output['FUTURE'] = output_value
    output = output.to_json(orient = 'records')
    return output