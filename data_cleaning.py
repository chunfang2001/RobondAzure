# -*- coding: utf-8 -*-
"""
Created on Thu Sep  2 17:55:14 2021

@author: chunfang
"""

from azureml.core import Run

new_run = Run.get_context()

ws = Run.get_context().experiment.workspace

from argparse import ArgumentParser as AP

parser = AP()
parser.add_argument('--cleanData', type=str)
parser.add_argument('--finance1path', type=str)
parser.add_argument('--finance2path',type=str)
parser.add_argument('--oprpath',type=str)
args = parser.parse_args()

import pandas as pd
import datetime
import requests
import json

from azureml.core import Datastore, Dataset

datastore = Datastore(ws, "finance")
financedt1 = [(datastore, 'BPAMERP*.csv')]
dt1 = Dataset.File.from_files(path=financedt1).to_path()
datastore = Datastore(ws, "finance")
financedt2 = [(datastore, 'BPAMERS*.csv')]
dt2 = Dataset.File.from_files(path=financedt2).to_path()

import os
# dataPrep = pd.DataFrame(columns=['hello'])
totaldf = pd.DataFrame()

oprpath = os.path.join(args.oprpath,'OPR.csv')
oprds = pd.read_csv(oprpath)

oprds['DATE'] = pd.to_datetime(oprds['Date'])
oprds = oprds.drop(columns=(['Date']))

for i in range(len(dt1)-1):
    # SPath
    spath = os.path.join(args.finance2path,dt2[i][1:])
    # PPath
    ppath = os.path.join(args.finance1path,dt1[i][1:])
    #nextPPath
    nextpath = os.path.join(args.finance1path,dt1[i+1][1:])
    
    df1 = pd.read_csv(spath)
    df1 = df1.filter(items=['STOCK CODE','RATING','NEXT COUPON RATE','COUPON FREQUENCY','MATURITY DATE','EXPECTED MATURITY DATE','NEXT PAYMENT DATE','PREVIOUS PAYMENT DATE','DAY COUNT BASIS','ISSUE DATE'])
    
    df2 = pd.read_csv(ppath)
    df2 = df2.filter(items=['EVAL MID YIELD','STOCK CODE','EVAL MID PRICE','MODIFIED DURATION','VALUE DATE','CONVEXITY','EVAL MID YIELD CHANGE'])
    
    df3 = pd.read_csv(nextpath)
    df3['FUTURE'] = df3['EVAL MID PRICE']
    df3 = df3.filter(items=['STOCK CODE','FUTURE'])
    
    df = pd.merge(df1, df2, on="STOCK CODE", how="right")
    df = pd.merge(df, df3, on="STOCK CODE", how="right")
    
    df['EXPECTED MATURITY DATE'] = pd.to_datetime(df['EXPECTED MATURITY DATE'])
    df['MATURITY DATE'] = pd.to_datetime(df['MATURITY DATE'])
    df['MATURITY DATE'] = df[["MATURITY DATE", "EXPECTED MATURITY DATE"]].max(axis=1)
    df = df.drop(columns=(['EXPECTED MATURITY DATE']))
    df['PREVIOUS PAYMENT DATE'].fillna(df['ISSUE DATE'], inplace=True)
    def conditions(s):
        if s['NEXT PAYMENT DATE'] == s['VALUE DATE']:
            return 0
        else:
            if s['DAY COUNT BASIS'] == "ACTACT":
                x = (pd.to_datetime(s['VALUE DATE'])-pd.to_datetime(s['PREVIOUS PAYMENT DATE'])).days/(pd.to_datetime(s['NEXT PAYMENT DATE'])-pd.to_datetime(s['PREVIOUS PAYMENT DATE'])).days/2
                return x*s['NEXT COUPON RATE']*50000
            else:
                x = (pd.to_datetime(s['VALUE DATE'])-pd.to_datetime(s['PREVIOUS PAYMENT DATE'])).days/365
                return x*s['NEXT COUPON RATE']*50000
    
    
    df['AINTEREST'] = df.apply(conditions, axis=1)
    df['DIF'] = df['FUTURE'] - df['EVAL MID PRICE']
    df["DAYS TO MATURITY"] = pd.to_datetime( df['MATURITY DATE']) - pd.to_datetime(df['VALUE DATE'])
    df["DAYS TO MATURITY"] = df["DAYS TO MATURITY"].dt.days
    
    options = ['AAA IS', 'AAA', 'AAA (S)', 'AAA IS (FG)', 'AAA (BG)', "AAA (FG)",'AA1','AA1 (S)','AA+','AA+ IS','AA2 (S)','AA2','AA','AA IS','AA3','AA3 (S)','AA- IS (CG)','AA- IS','AA-']

    df = df.reset_index(drop=True)
    my_date = str(pd.to_datetime(df['VALUE DATE'])[0])[:10]
    headers = {'Accept':'application/vnd.BNM.API.v1+json'}
    res = requests.get('https://api.bnm.gov.my/public/gov-sec-yield?date={}'.format(my_date), headers=headers)
    
    x = json.loads(res.text)
    b = pd.DataFrame.from_dict(x)
    gg = b.iloc[:1]['data'][0]
    government_bond = pd.DataFrame.from_dict(gg)
    
    def get_maturity_date(s):
        sad = datetime.datetime(int(s.maturity_year), int(s.maturity_month), 15)
        return sad
    
    government_bond['MATURITY DATE'] = government_bond.apply(get_maturity_date, axis=1)
    
    def find_credit_spread(s):
        if s['RATING'] == 'NR(LT)':
            return None
        x = abs(pd.to_datetime(s['MATURITY DATE']) - government_bond['MATURITY DATE'])
        x = pd.Series.sort_values(x)
        return s['EVAL MID YIELD'] - float(government_bond['tra_yie_close'][x.index[0]])

    df['CREDIT SPREAD'] = df.apply(find_credit_spread, axis=1)
    
    df = df[df['RATING'].isin(options)]
    df = df.dropna()
    
    
    df = df[['STOCK CODE','RATING','NEXT COUPON RATE','AINTEREST','COUPON FREQUENCY','EVAL MID PRICE','EVAL MID YIELD','MATURITY DATE','MODIFIED DURATION','FUTURE','DIF','VALUE DATE','CONVEXITY','EVAL MID YIELD CHANGE','DAYS TO MATURITY','CREDIT SPREAD']]
    totaldf = totaldf.append(df)
    
totaldf['DATE'] = pd.to_datetime(totaldf['VALUE DATE'])
totaldf = pd.merge(totaldf,oprds,on="DATE")
totaldf['OPR MOVEMENT'] = totaldf['Overnight']
totaldf = totaldf.drop(columns=['Overnight', '1 Week','1 Month','3 Month','6 Month','1 Year','DATE'])

os.makedirs(args.cleanData, exist_ok=True)
path = os.path.join(args.cleanData, 'cleaned.csv')
totaldf.to_csv(path, index=False)

new_run.complete()