# -*- coding: utf-8 -*-
"""
Created on Thu Sep  2 17:45:18 2021

@author: chunfang
"""
from azureml.core import Run
from azureml.core import Environment

new_run = Run.get_context()

from argparse import ArgumentParser as AP

ws = Run.get_context().experiment.workspace

parser = AP()
parser.add_argument('--model2', type=str)
args = parser.parse_args()

myenv = Environment.get(ws, "MyEnvironment")

from azureml.core.model import InferenceConfig
inference_config = InferenceConfig(
    environment=myenv,
    source_directory=".",
    entry_script="score.py",
)

from azureml.core.webservice import AksWebservice
from azureml.core.model import Model

deploy_config = AksWebservice.deploy_configuration(cpu_cores=1,
                                                   memory_gb=1)
model = ws.models['test']
X_scaler = ws.models['X_scaler']

production_cluster = ws.compute_targets['test']
service = Model.deploy(
    ws,
    "myservice",
    [model,X_scaler],
    inference_config,
    deploy_config,
    production_cluster,
    overwrite=True
)
service.wait_for_deployment(show_output=True)

new_run.complete()