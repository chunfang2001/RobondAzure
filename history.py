# -*- coding: utf-8 -*-
"""
Created on Sun Sep  5 15:23:27 2021

@author: chunfang
"""

from azureml.core import Run

new_run = Run.get_context()

ws = Run.get_context().experiment.workspace

from argparse import ArgumentParser as AP

parser = AP()
parser.add_argument('--finance1path', type=str)
parser.add_argument('--finance2path', type=str)
parser.add_argument('--history', type=str)
args = parser.parse_args()

import pandas as pd

from azureml.core import Datastore, Dataset

datastore = Datastore(ws, "finance")
financedt1 = [(datastore, 'BPAMERP*.csv')]
dt1 = Dataset.File.from_files(path=financedt1).to_path()
datastore = Datastore(ws, "finance")
financedt2 = [(datastore, 'BPAMERS*.csv')]
dt2 = Dataset.File.from_files(path=financedt2).to_path()

import os

a=len(dt1)-1
core_path = os.path.join(args.finance1path,dt1[a][1:])
core_path2 = os.path.join(args.finance2path,dt2[a][1:])

data = pd.read_csv(core_path)
data2 = pd.read_csv(core_path2)

data = pd.merge(data,data2,on='STOCK CODE',how='left')

data['EXPECTED MATURITY DATE'] = pd.to_datetime(data['EXPECTED MATURITY DATE'])
data['MATURITY DATE'] = pd.to_datetime(data['MATURITY DATE'])
data['MATURITY DATE'] = data[["MATURITY DATE", "EXPECTED MATURITY DATE"]].max(axis=1)
data = data.drop(columns=(['EXPECTED MATURITY DATE']))

data['VALUE DATE'] = pd.to_datetime(data['VALUE DATE'])
data['MATURITY DATE'] = pd.to_datetime(data['MATURITY DATE'])

data = data[~((data['MATURITY DATE'].dt.year == data['VALUE DATE'].dt.year) & (data['MATURITY DATE'].dt.month == data['VALUE DATE'].dt.month))]

data = data.reset_index(drop=True)
date = data['VALUE DATE'][0]
date_obj = pd.to_datetime(date,infer_datetime_format=True)
filename = date_obj.strftime("%Y")+date_obj.strftime('%m')+"2"

totaldf = pd.DataFrame()

totaldf['STOCK CODE'] = data['STOCK CODE']

for i in range(len(dt1)):
    path = os.path.join(args.finance1path,dt1[i][1:])
    df = pd.read_csv(path)
    date = df['VALUE DATE'][0] 
    date_obj = pd.to_datetime(date,infer_datetime_format=True)
    column_name = date_obj.strftime("%b")+" "+date_obj.strftime('%Y')
    df = df[['STOCK CODE','EVAL MID PRICE']]
    df[column_name] = df['EVAL MID PRICE']
    df = df.drop(columns=['EVAL MID PRICE'])
    totaldf = pd.merge(totaldf,df,how='left',on='STOCK CODE')
    
save_path = os.path.join(args.history,filename+".csv")

totaldf.to_csv(save_path,index = False)

new_run.complete()