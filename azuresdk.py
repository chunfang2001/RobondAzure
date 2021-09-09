from azureml.core import Workspace

ws = Workspace.from_config(".config")

# =============================================================================
# 
# =============================================================================
from azureml.core import Environment
from azureml.core.environment import CondaDependencies

# Create the environment
myenv = Environment(name="MyEnvironment")

# Create the dependencies object
myenv_dep = CondaDependencies.create(conda_packages=['scikit-learn','pandas','numpy','xgboost'])

myenv.python.conda_dependencies = myenv_dep
myenv.register(ws)
# =============================================================================
# 
# =============================================================================
cluster_name = "pipeline-cluster"

from azureml.core.compute import AmlCompute
compute_config = AmlCompute.provisioning_configuration(
                                    vm_size='STANDARD_D11_V2', 
                                    max_nodes=2)


from azureml.core.compute import ComputeTarget
compute_cluster = ComputeTarget.create(ws, cluster_name, compute_config)

compute_cluster.wait_for_completion()
# =============================================================================
# 
# =============================================================================
from azureml.core.runconfig import RunConfiguration
run_config = RunConfiguration()

run_config.target = compute_cluster
run_config.environment = myenv
# =============================================================================
# 
# =============================================================================
# Define Pipeline steps
from azureml.pipeline.steps import PythonScriptStep
from azureml.pipeline.core  import PipelineData
from azureml.core import Datastore,Dataset
from azureml.data import OutputFileDatasetConfig

# cleanData = PipelineData('clean',datastore=ws.get_default_datastore())

dataFolder = PipelineData('test', datastore=ws.get_default_datastore())
model = PipelineData('model',datastore=ws.get_default_datastore())
model2 = PipelineData('model',datastore=ws.get_default_datastore())
store = Datastore(ws,"financeoutput")

output = OutputFileDatasetConfig(destination=(store, '/processData'))
cleanData = PipelineData('clean',datastore=ws.get_default_datastore())

predictData = PipelineData('predictData',datastore=ws.get_default_datastore())
futureData = PipelineData('futureData',datastore=ws.get_default_datastore())
bondReturnData = PipelineData('bondReturnData',datastore=ws.get_default_datastore())
allOutput = OutputFileDatasetConfig(destination=(store, '/output'))
history = OutputFileDatasetConfig(destination=(store, '/output'))
bondreturn = OutputFileDatasetConfig(destination=(store, '/output'))
metrics = OutputFileDatasetConfig(destination=(store, '/metrics'))

datastore = Datastore(ws, "finance")
financedt1 = [(datastore, 'BPAMERP*.csv')] # testdata contains files test1.txt, test2.txt
fpath1 = Dataset.File.from_files(path=financedt1).as_mount()
financedt2 = [(datastore, 'BPAMERS*.csv')]
fpath2 = Dataset.File.from_files(path=financedt2).as_mount()
opr = [(datastore, 'OPR*.csv')]
fpath3 = Dataset.File.from_files(path=opr).as_mount()


dataCleaning_step = PythonScriptStep(name='data_cleaning',
                            source_directory='.',
                            script_name='data_cleaning.py',
                            runconfig=run_config,
                            outputs=[cleanData],
                            allow_reuse=False,
                            arguments=['--cleanData',cleanData,'--finance1path',fpath1,'--finance2path',fpath2,'--oprpath',fpath3])

bondreturn_step = PythonScriptStep(name='bond return step',
                            source_directory='.',
                            script_name='bondreturn.py',
                            runconfig=run_config,
                            outputs=[bondreturn,bondReturnData],
                            allow_reuse=False,
                            arguments=['--bondreturn',bondreturn,'--finance1path',fpath1,'--finance2path',fpath2,'--bondReturnData',bondReturnData])

dataPrep_step = PythonScriptStep(name='data_preparation',
                                  source_directory='.',
                                  script_name='dataprep.py',
                                  inputs=[cleanData],
                                  outputs=[dataFolder],
                                  runconfig=run_config,
                                  allow_reuse=False,
                                  arguments=['--datafolder', dataFolder,'--cleanData',cleanData])

history_step = PythonScriptStep(name='history preparation',
                                  source_directory='.',
                                  script_name='history.py',
                                  runconfig=run_config,
                                  allow_reuse=False,
                                  arguments=['--finance1path',fpath1,'--finance2path',fpath2,'--history',history])

dataprediction_step = PythonScriptStep(name='data_prediction preparation',
                                  source_directory='.',
                                  script_name='dataprediction.py',
                                  runconfig=run_config,
                                  outputs=[predictData],
                                  allow_reuse=False,
                                  arguments=['--predictData',predictData,'--finance1path',fpath1,'--finance2path',fpath2,'--oprpath',fpath3])

batchprediction_step = PythonScriptStep(name='batch_prediction preparation',
                                  source_directory='.',
                                  script_name='batchprediction.py',
                                  runconfig=run_config,
                                  inputs=[predictData,model2],
                                  outputs=[futureData],
                                  allow_reuse=False,
                                  arguments=['--predictData',predictData,'--futureData',futureData,'--model2',model2])

processData_step = PythonScriptStep(name='process data',
                                  source_directory='.',
                                  script_name='process.py',
                                  runconfig=run_config,
                                  inputs=[futureData,bondReturnData],
                                  allow_reuse=False,
                                  arguments=['--futureData',futureData,'--allOutput',allOutput,'--bondReturnData',bondReturnData])

train_step    = PythonScriptStep(name='model_training',
                                  source_directory='.',
                                  script_name='train.py',
                                  inputs=[dataFolder],
                                  outputs=[model],
                                  runconfig=run_config,
                                  allow_reuse=False,
                                  arguments=['--datafolder', dataFolder,'--model',model,'--output',output,'--metrics',metrics])
                                  
register_step    = PythonScriptStep(name='model_register',
                                  source_directory='.',
                                  script_name='register.py',
                                  inputs=[model],
                                  outputs=[model2],
                                  runconfig=run_config,
                                  allow_reuse=False,
                                  arguments=['--model',model,'--model2',model2])

deploy_step    = PythonScriptStep(name='model_deployment',
                                  source_directory='.',
                                  script_name='deploy.py',
                                  inputs=[model2],
                                  runconfig=run_config,
                                  allow_reuse=False,
                                  arguments=['--model2',model2])                          
# =============================================================================

# =============================================================================

# Configure and build the pipeline
steps = [dataprediction_step,batchprediction_step,processData_step,dataCleaning_step,dataPrep_step,train_step,register_step,deploy_step,history_step]


from azureml.pipeline.core import Pipeline
new_pipeline = Pipeline(workspace=ws, steps=steps)

from azureml.core import Experiment

new_experiment = Experiment(workspace=ws, name='PipelineExp01')
new_pipeline_run = new_experiment.submit(new_pipeline)

new_pipeline_run.wait_for_completion(show_output=True)