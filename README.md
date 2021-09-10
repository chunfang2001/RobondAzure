# RobondAzure 
This is an automated Azure Machine Learning Pipeline configuration

# Azure connection file 

* .azureml - contains azure machine learning workspace details that use to connect with azure machine learning
* azuresdk.py - connects with azure workspace and create pipeline in azure cloud

# Azure pipeline related file

* batchprediction.py - pipeline step that is used to carry out batch prediction
* bondreturn.py - pipeline step that is used to calculate bond return in history
* data_cleaning.py - pipeline step that is used to clean raw dataset to become one dataset
* datapreparation.py - pipeline step that is used to prepare data for prediction
* dataprep.py - pipeline step that is used to encode dataset for training
* deploy.py - pipeline step that is used to deploy the registered model in azure cloud by azure kurbenetes service
* history.py - pipeline step that is used to find out history bond price from raw dataset
* process.py - pipeline step that is used to process the predicted data to calculate bond return vs volatility ratio
* register.py  pipeline step that is used to register the model trained in azure cloud
* train.py - pipeline step that is used to train model based on cleaned and encoded dataset

# Azure kurbenetes service file

* score.py - entry script that is used in azure kurbenetes service deployment
