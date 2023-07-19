from infrastructure_workbench.integrations.[OBJECT] import [OBJECT]


from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

import os
import dill
import codecs


dag = DAG('[DAG_NAME]', description='[OBJECT] DAG',
		schedule_interval='0 12 * * *',
		start_date=datetime(2017, 3, 20), 
		catchup=False,
		render_template_as_native_obj=True)
		  
		  
def setup(**kwargs):
	ti = kwargs['ti']
	obj = [OBJECT]()
	obj.authenticate()
	obj.setEndpoint('[SOURCE_ENDPOINT]')
	if len("[SOURCE_OBJECT]") > 0:
		obj.buildQuery("[SOURCE_OBJECT]")
	else:
		obj.setQuery("[DAG_QUERY]")
	obj.destroyS3Client()
	if callable(getattr(obj, 'destroyAthenaClient', None)):
		obj.destroyAthenaClient()
	ti.xcom_push(key='obj', value=dill.dumps(obj))


def runQuery(**kwargs):
	ti = kwargs['ti']
	obj = dill.loads(bytes(ti.xcom_pull(key='obj', task_ids=['Setup'])[0]))
	obj.createS3Client()
	if callable(getattr(obj, 'createAthenaClient', None)):
		obj.createAthenaClient()
	output = obj.runQuery()
	ti.xcom_push(key='execution_id', value=obj.execution_id)
	return output

def transportLocal(ti):
	obj = dill.loads(bytes(ti.xcom_pull(key='obj', task_ids=['Setup'])[0]))
	obj.createS3Client()
	if callable(getattr(obj, 'createAthenaClient', None)):
		obj.createAthenaClient()
	obj.execution_id = str(ti.xcom_pull(key='execution_id', task_ids=['Running_Query'])[0])
	obj.transferCloudToLocal(obj.output_cloud_location + obj.execution_id + ".csv")
	ti.xcom_push(key='local_location', value=obj.local_file)
	return obj.local_file
	
def sliceFile(ti):
	obj = dill.loads(bytes(ti.xcom_pull(key='obj', task_ids=['Creating_Destinations'])[0]))
	#file_location = str(ti.xcom_pull(key='local_location', task_ids=['Transporting_Data_S3-Local'])[0])
	file_location = obj.local_file
	obj.createS3Client()
	destinations = obj.destinations
	for x in destinations:
		engine = type(x).__name__
		if engine in [DAG_DEST]:
			obj.engineSliceFile(x.local_file, engine)

def compressFile(ti):
	obj = dill.loads(bytes(ti.xcom_pull(key='obj', task_ids=['Creating_Destinations'])[0]))
	#file_location = str(ti.xcom_pull(key='local_location', task_ids=['Transporting_Data_S3-Local'])[0])
	file_location = obj.data_directory
	obj.createS3Client()
	destinations = obj.destinations
	for x in destinations:
		engine = type(x).__name__
		if engine in [DAG_DEST] and x.compression is not None:
			obj.compressDirectory(x.data_directory, algo=x.compression)
	
def transportCloud(ti):
	obj = dill.loads(bytes(ti.xcom_pull(key='obj', task_ids=['Creating_Destinations'])[0]))
	obj.createS3Client()
	dest = [DAG_DEST]
	for x in dest:
		obj.transferLocalToCloud(engine=x)

def createDestination(ti):
	obj = dill.loads(bytes(ti.xcom_pull(key='obj', task_ids=['Setup'])[0]))
	obj.createS3Client()
	destinations = [DAG_DEST]
	[DESTINATION_ASSIGNMENT]
	#for x in destinations:
	 #   obj.createDestination(destination=x,local_location=obj.data_directory, cloud_location=obj.cloud_directory, schema='[DAG_DEST_SCHEMA]', table='[DAG_DEST_TABLE]', primary_key='[DAG_DEST_P_KEY]', destination_s3='[DAG_DEST_S3]')
	obj.destroyS3Client()
	if callable(getattr(obj, 'destroyAthenaClient', None)):
		obj.destroyAthenaClient()
	ti.xcom_push(key='obj', value=dill.dumps(obj))

def partitionData(ti):
	obj = dill.loads(bytes(ti.xcom_pull(key='obj', task_ids=['Creating_Destinations'])[0]))
	obj.createS3Client()
	destinations = obj.destinations
	for x in destinations:
		engine = type(x).__name__
		if engine in [DAG_DEST]:
			obj.partitionData(x.local_file, partition_type=x.destination_partition_type, partition_keys=x.destination_s3_partition_key, partitions=x.destination_s3_partition_value, engine=engine)


def destinationImport(ti):
	obj = dill.loads(bytes(ti.xcom_pull(key='obj', task_ids=['Creating_Destinations'])[0]))
	obj.createS3Client()
	destinations = obj.destinations #CHANGE THIS TO RUN ASYNC - change to obj.runImport()
	for x in destinations:
		x.aws_access_key = obj.aws_access_key
		x.aws_secret_key = obj.aws_secret_key
		if hasattr(x, 's3_client'):
			x.setS3Client(obj.s3_client)
		x.runImport()

	
setup = PythonOperator(
						task_id="Setup",
						python_callable=setup, 
						provide_context=True, 
						dag=[DAG_INSTANCE])

query_operator = PythonOperator(
						task_id="Running_Query",
						python_callable=runQuery, 
						provide_context=True, 
						dag=[DAG_INSTANCE])
						
						
transport_operator = PythonOperator(
						task_id="Transporting_Data_S3-Local",
						python_callable=transportLocal, 
						provide_context=True, 
						dag=[DAG_INSTANCE])

slice_operator = PythonOperator(
						task_id="Slicing_Data", 
						python_callable=sliceFile, 
						provide_context=True, 
						dag=[DAG_INSTANCE])

compress_operator = PythonOperator(
						task_id="Compressing_Data", 
						python_callable=compressFile, 
						provide_context=True, 
						dag=[DAG_INSTANCE])
						
transport_cloud_opp = PythonOperator(
						task_id="Transporting_Data_Local-S3",
						python_callable=transportCloud, 
						provide_context=True, 
						dag=[DAG_INSTANCE])

partition_operator = PythonOperator(
						task_id="Partitioning_Data",
						python_callable=partitionData, 
						provide_context=True, 
						dag=[DAG_INSTANCE])
						
create_destinations = PythonOperator(
						task_id="Creating_Destinations",
						python_callable=createDestination,
						provide_context=True, 
						dag=[DAG_INSTANCE])


import_destinations = PythonOperator(
						task_id="Importing_to_Destinations",
						python_callable=destinationImport,
						provide_context=True, 
						dag=[DAG_INSTANCE])
	
[WORKFLOW]