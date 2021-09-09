# -*- coding: utf-8 -*-
"""
Created on Sun Sep  5 11:16:02 2021

@author: chunfang
"""

from azureml.core import Run

new_run = Run.get_context()

ws = Run.get_context().experiment.workspace

from argparse import ArgumentParser as AP

parser = AP()
parser.add_argument('--predictData', type=str)
parser.add_argument('--futureData', type=str)
parser.add_argument('--model2', type=str)
args = parser.parse_args()

import pandas as pd

import os

from azureml.core import Model

path = os.path.join(args.predictData,'predictData.csv')
df = pd.read_csv(path)

newdf = pd.DataFrame()

for i in df.columns:
    newdf[i] = df[i]
df = df.drop(columns=(['STOCK CODE','ISIN CODE','STOCK NAME','MATURITY DATE','VALUE DATE','EVAL MID YIELD CHANGE','CONVEXITY']))

all_columns =['NEXT COUPON RATE','AINTEREST','COUPON FREQUENCY','EVAL MID PRICE',
            'EVAL MID YIELD','MODIFIED DURATION','DAYS TO MATURITY','CREDIT SPREAD',
            'OPR MOVEMENT','RATING_AA','RATING_AA IS','RATING_AA+','RATING_AA+ IS',
            'RATING_AA-','RATING_AA- IS','RATING_AA- IS (CG)','RATING_AA1','RATING_AA1 (S)',
            'RATING_AA2','RATING_AA2 (S)','RATING_AA3','RATING_AA3 (S)','RATING_AAA',
            'RATING_AAA (BG)','RATING_AAA (FG)','RATING_AAA (S)','RATING_AAA IS',
            'RATING_AAA IS (FG)']

schema = pd.DataFrame(columns=all_columns)
rel_cols = schema.columns
    
df = pd.get_dummies(df)

missing_cols = rel_cols.difference(df.columns)
     
for i in missing_cols:
    df[i] = 0
    
df = df[all_columns]

path = Model.get_model_path('test',_workspace=ws)
path2 = Model.get_model_path('X_scaler',_workspace=ws)

import joblib

predictor = joblib.load(path)
X_scaler = joblib.load(path2)

scaled_data = X_scaler.transform(df)
output = predictor.predict(scaled_data)

newdf['PREDICTION'] = output

os.makedirs(args.futureData, exist_ok=True)
path = os.path.join(args.futureData, 'predictResult.csv')
newdf.to_csv(path, index=False)
new_run.complete()
