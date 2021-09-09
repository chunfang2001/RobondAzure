# -*- coding: utf-8 -*-
"""
Created on Sun Sep  5 20:58:58 2021

@author: chunfang
"""

from azureml.core import Run

new_run = Run.get_context()

ws = Run.get_context().experiment.workspace

from argparse import ArgumentParser as AP

parser = AP()
parser.add_argument('--bondreturn', type=str)
parser.add_argument('--finance1path', type=str)
parser.add_argument('--finance2path',type=str)
parser.add_argument('--bondReturnData',type=str)
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

a = len(dt1)-1

bondpath = os.path.join(args.finance1path,dt1[a][1:])
bondpath2 = os.path.join(args.finance2path,dt2[a][1:])
totaldf = pd.DataFrame()

allbond = pd.read_csv(bondpath)
allbond2 = pd.read_csv(bondpath2)

allbond = pd.merge(allbond,allbond2,on='STOCK CODE',how='left')

allbond['VALUE DATE'] = pd.to_datetime(allbond['VALUE DATE'])
allbond['MATURITY DATE'] = pd.to_datetime(allbond['MATURITY DATE'])
allbond['EXPECTED MATURITY DATE'] = pd.to_datetime(allbond['EXPECTED MATURITY DATE'])
allbond['MATURITY DATE'] = pd.to_datetime(allbond['MATURITY DATE'])
allbond['MATURITY DATE'] = allbond[["MATURITY DATE", "EXPECTED MATURITY DATE"]].max(axis=1)
allbond = allbond.drop(columns=(['EXPECTED MATURITY DATE']))

allbond = allbond[~((allbond['MATURITY DATE'].dt.year == allbond['VALUE DATE'].dt.year) & (allbond['MATURITY DATE'].dt.month == allbond['VALUE DATE'].dt.month))]

totaldf['STOCK CODE'] = allbond['STOCK CODE']

allbond = allbond.reset_index(drop=True)
date = allbond['VALUE DATE'][0]
date_obj = pd.to_datetime(date,infer_datetime_format=True)
filename = date_obj.strftime("%Y")+date_obj.strftime('%m')+"3"

for i in range(len(dt1)-1):
    # SPath
    spath = os.path.join(args.finance2path,dt2[i][1:])
    # PPath
    ppath = os.path.join(args.finance1path,dt1[i][1:])
    #nextPPath
    nextpath = os.path.join(args.finance1path,dt1[i+1][1:])
    
    df1 = pd.read_csv(spath)
    df1 = df1.filter(items=['STOCK CODE','RATING','NEXT COUPON RATE','COUPON FREQUENCY','MATURITY DATE','NEXT PAYMENT DATE','PREVIOUS PAYMENT DATE','DAY COUNT BASIS'])
    
    df2 = pd.read_csv(ppath)
    df2 = df2.filter(items=['EVAL MID YIELD','STOCK CODE','EVAL MID PRICE','MODIFIED DURATION','VALUE DATE','CONVEXITY','EVAL MID YIELD CHANGE'])
    
    df3 = pd.read_csv(nextpath)
    df3['FUTURE'] = df3['EVAL MID PRICE']
    df3 = df3.filter(items=['STOCK CODE','FUTURE'])
    
    df = pd.merge(df1, df2, on="STOCK CODE", how="left")
    df = pd.merge(df, df3, on="STOCK CODE", how="left")
    
    date = df2['VALUE DATE'][0] 
    date_obj = pd.to_datetime(date,infer_datetime_format=True)
    column_name = date_obj.strftime("%b")+" "+date_obj.strftime('%Y')  
    
    df[column_name] = df['NEXT COUPON RATE']/df['EVAL MID PRICE'] + (df['FUTURE'] - df['EVAL MID PRICE'])/df['EVAL MID PRICE'] + (-df['MODIFIED DURATION'] * df['EVAL MID YIELD CHANGE']) + (0.5 * df['CONVEXITY'] * (df['EVAL MID YIELD CHANGE'])**2)
    df = df[['STOCK CODE',column_name]]
    totaldf = pd.merge(totaldf,df,how='left',on='STOCK CODE')
   
savepath = os.path.join(args.bondreturn,filename+".csv")

os.makedirs(args.bondReturnData, exist_ok=True)
nextpath = os.path.join(args.bondReturnData,"bondReturn.csv")
totaldf.to_csv(savepath,index = False)
totaldf.to_csv(nextpath,index = False)

new_run.complete()