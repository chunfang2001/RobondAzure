# -*- coding: utf-8 -*-
"""
Created on Sun Sep  5 13:29:32 2021

@author: chunfang
"""

from azureml.core import Run

new_run = Run.get_context()

ws = Run.get_context().experiment.workspace

from argparse import ArgumentParser as AP

parser = AP()
parser.add_argument('--futureData', type=str)
parser.add_argument('--allOutput', type=str)
parser.add_argument('--bondReturnData', type=str)
args = parser.parse_args()

import pandas as pd

import os

path = os.path.join(args.futureData, 'predictResult.csv')
data = pd.read_csv(path)
path2 = os.path.join(args.bondReturnData,'bondReturn.csv')
data2 = pd.read_csv(path2)

data2['VOLATILITY']=data2.std(axis=1,ddof=0)
data2 = data2[['STOCK CODE','VOLATILITY']]
df = pd.DataFrame()

data= data.reset_index(drop=True)
date = data['VALUE DATE'][0]
date_obj = pd.to_datetime(date,infer_datetime_format=True)
filename = date_obj.strftime("%Y")+date_obj.strftime('%m')+"1"

df['STOCK CODE'] = data['STOCK CODE']
df['ISIN CODE'] = data['ISIN CODE']
df['STOCK NAME'] = data['STOCK NAME']
df['RATING'] = data['RATING']
df['EVAL MID PRICE'] = data['EVAL MID PRICE']
df['MATURITY DATE'] = data['MATURITY DATE']
df['NEXT COUPON RATE'] = data['NEXT COUPON RATE']

df['PREDICTION'] = data['PREDICTION']
df['BOND RETURN'] = data['NEXT COUPON RATE']/data['EVAL MID PRICE'] + (data['PREDICTION'] - data['EVAL MID PRICE'])/data['EVAL MID PRICE'] + (-data['MODIFIED DURATION'] * data['EVAL MID YIELD CHANGE']) + (0.5 * data['CONVEXITY'] * (data['EVAL MID YIELD CHANGE'])**2)

df = pd.merge(df, data2,on='STOCK CODE',how='left')

def find_ratio(s):
    if s['VOLATILITY'] == 0:
        return 0
    return s['BOND RETURN']/s['VOLATILITY']

df['RATIO'] = pd.Series()
df['RATIO'] = df.apply(find_ratio, axis=1)

df = df.sort_values(by="RATIO",ascending=False)

df = df.reset_index(drop=True)
df.reset_index(inplace=True)
df = df.rename(columns = {'index':'RANK'})
df['RANK'] += 1

os.makedirs(args.allOutput, exist_ok=True)
output_path = os.path.join(args.allOutput, filename+".csv")
data = df.to_csv(output_path,index=False)

new_run.complete()