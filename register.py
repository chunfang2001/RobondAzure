# -*- coding: utf-8 -*-
"""
Created on Thu Sep  2 15:34:18 2021

@author: chunfang
"""

from azureml.core import Run

new_run = Run.get_context()

from argparse import ArgumentParser as AP

ws = Run.get_context().experiment.workspace

parser = AP()
parser.add_argument('--model', type=str)
parser.add_argument('--model2', type=str)
args = parser.parse_args()


import os

path = os.path.join(args.model, 'modeltest.pkl')
path2 = os.path.join(args.model, 'X_scaler.pkl')

from azureml.core import Model

Model.register(workspace = ws,
               model_path=path,
               model_name="test")

Model.register(workspace = ws,
               model_path=path2,
               model_name="X_scaler")

new_run.complete()