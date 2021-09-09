# -*- coding: utf-8 -*-
"""
Created on Wed Sep  1 23:13:30 2021

@author: chunfang
"""

from azureml.core import Run

new_run = Run.get_context()

from argparse import ArgumentParser as AP

ws = Run.get_context().experiment.workspace

parser = AP()
parser.add_argument('--datafolder', type=str)
parser.add_argument('--cleanData', type=str)
args = parser.parse_args()

import pandas as pd

import os
input_path = os.path.join(args.cleanData,'cleaned.csv')

df = pd.read_csv(input_path)

df = df.drop(['DIF','VALUE DATE','MATURITY DATE','STOCK CODE','CONVEXITY','EVAL MID YIELD CHANGE'],axis=1)
df = pd.get_dummies(df,columns=(['RATING']))

dataPrep = df
all_cols = dataPrep.columns
dataNull = dataPrep.isnull().sum()

os.makedirs(args.datafolder, exist_ok=True)

# Create the path
path = os.path.join(args.datafolder, 'defaults_prep.csv')

# Write the data preparation output as csv file
dataPrep.to_csv(path, index=False)


# Log null values
for column in all_cols:
    new_run.log(column, dataNull[column])


# Complete the run
new_run.complete()
