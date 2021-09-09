# -*- coding: utf-8 -*-
"""
Created on Thu Sep  2 14:15:21 2021

@author: chunfang
"""

from azureml.core import Run

new_run = Run.get_context()

import pandas as pd

from argparse import ArgumentParser as AP
from azureml.core import Dataset, Datastore
from statistics import mean

ws = Run.get_context().experiment.workspace

parser = AP()
parser.add_argument('--datafolder', type=str)
parser.add_argument('--model', type=str)
parser.add_argument('--output',type=str)
parser.add_argument('--metrics', type=str)
args = parser.parse_args()


import os

path = os.path.join(args.datafolder, 'defaults_prep.csv')
path2 = os.path.join(args.output, 'encodedData.csv')
dataPrep = pd.read_csv(path)

temp = dataPrep['FUTURE']
dataPrep = dataPrep.drop(columns=(['FUTURE']))
dataPrep['FUTURE'] = temp

dataPrep.to_csv(path2)
store = Datastore(ws,name='financeoutput')
train_ds = Dataset.Tabular.from_delimited_files(path=(store,'processData/encodedData.csv'))

train_ds = train_ds.register(workspace = ws,
                            name = 'train_ds',
                            create_new_version = True)

mean_of_dif = mean(abs(dataPrep['FUTURE'] - dataPrep['EVAL MID PRICE']).values)
dataPrep = dataPrep[(abs(dataPrep['FUTURE'] - dataPrep['EVAL MID PRICE'])) < 10 * mean_of_dif]

X = dataPrep.iloc[:, 0:28].values
y = dataPrep.iloc[:, 28].values

from sklearn.model_selection import train_test_split

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=0)

y_train = y_train.reshape(-1, 1)

from sklearn.preprocessing import MinMaxScaler

X_scaler = MinMaxScaler()
X_train = X_scaler.fit_transform(X_train)
X_test = X_scaler.transform(X_test)
y_train = y_train.ravel()

from xgboost import XGBRegressor

regressor = XGBRegressor(objective='reg:squarederror',n_estimators=225)
regressor.fit(X_train, y_train)
y_pred = regressor.predict(X_test)

from sklearn import metrics
import numpy as np

MAE = [metrics.mean_absolute_error(y_test, y_pred)]
MSE = [metrics.mean_absolute_error(y_test, y_pred)]
RSME = [np.sqrt(metrics.mean_squared_error(y_test, y_pred))]

metrics_output = pd.DataFrame()
metrics_output['Mean Absolute Error'] = [metrics.mean_absolute_error(y_test, y_pred)]
metrics_output['Mean Squared Error'] = [metrics.mean_squared_error(y_test, y_pred)]
metrics_output['Root Mean Squared Error'] = [np.sqrt(metrics.mean_squared_error(y_test, y_pred))]

os.makedirs(args.metrics, exist_ok=True)
metrics_path = os.path.join(args.metrics,'metrics.csv')
metrics_output.to_csv(metrics_path)

os.makedirs(args.model, exist_ok=True)
modelpath = os.path.join(args.model, 'modeltest.pkl')
X_scalerpath = os.path.join(args.model, "X_scaler.pkl")
y_scalerpath = os.path.join(args.model, "y_scaler.pkl")

import joblib
joblib.dump(X_scaler, X_scalerpath)  
joblib.dump(regressor, modelpath)

new_run.log("TotalObservations", len(dataPrep))

new_run.complete()