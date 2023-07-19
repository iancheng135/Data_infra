import json
import os
import shutil
import fileinput


def destinationRequiredOperator(destination, current_operator):
	return current_operator in destinations[destination]['required_operators']

def updateCurrentOperator(line, current_operator):
	if " = PythonOperator(" in line:
		return line.split(" = PythonOperator(")[0].strip()
	return current_operator

def updateCurrentFunction(line, current_function):
	if "def " in line:
		return line.split("(")[0].strip().replace("def ", "")
	return current_function

def findOperatorFunctionMapping():
	mapping = {}
	with open("../templates/dag_template.py", "r") as f:
		lines = f.readlines()
		current_operator = None
		for x in lines:
			current_operator = updateCurrentOperator(x, current_operator)
			if current_operator is not None:
				if "python_callable=" in x:
					function = x.split("python_callable=")[1].split(",")[0]
					mapping[function] = current_operator
	return mapping



config_file = open("../config/dag_config.json")
sources_file = open("../config/sources.json")
destinations_file = open("../config/destinations.json")
sources = json.load(sources_file)
destinations = json.load(destinations_file)
op_func_mapping = findOperatorFunctionMapping()
config = json.load(config_file)
for dag_name in config:
	new_filename = "../dags/" + dag_name + ".py"
	shutil.copyfile("../templates/dag_template.py", new_filename)
	workflow_nodes = ['setup']
	source_required_nodes = sources[config[dag_name]['source']]['required_operators']
	
	obj_instance_variables = []
	variable_accepted_types = ['int', 'str']
	for var in config[dag_name]:
		ignore_vars = ['object', 'source', 'query']
		if config[dag_name][var] is None:
			continue
		wrap_quotes = (isinstance(config[dag_name][var], str))
		if (var not in ignore_vars) and (isinstance(config[dag_name][var], int) or isinstance(config[dag_name][var], str)):
			if wrap_quotes:
				var_assignment = "obj." + var + "='" + config[dag_name][var] + "'"
			else:
				var_assignment = "obj." + var + "=" + str(config[dag_name][var])

			obj_instance_variables.append(var_assignment)

	dests = config[dag_name]['destination']
	dest_assignment = []
	destination_required_nodes = []
	for k,v in dests.items():
		destination_required_nodes.append(destinations[k]['required_operators'])
		assignment = "destination = obj.createDestination(destination='" + k + "',local_location=obj.data_directory, cloud_location=obj.cloud_directory)"
		dest_assignment.append(assignment)
		for x,y in v.items():
			assignment = "destination." + x + "='" + y + "'"
			dest_assignment.append(assignment)
	dest_op = ["create_destinations"]
	dest_import = ['import_destinations']
	workflows = []
	for x in destination_required_nodes:
		workflows.append(">>". join(workflow_nodes + source_required_nodes + dest_op + x + dest_import ))
	final_workflow = '\n'.join(workflows)

	macro_mapping = {
		"source":"",
		"destination": "",
		"schema": "",
		"table": "",
		"compression": "",
		"query": "",
		"object": "",
		"table_primary_key": "",
		"destination_s3": "",
		"destination_partition_type": "",
		"destination_s3_partition_value": "",
		"destination_s3_partition_key":"",
		"source_endpoint":""
	}

	for x,y in macro_mapping.items():
		if x in config[dag_name]:
			macro_mapping[x] = config[dag_name][x]
	
	
	with fileinput.input(new_filename, inplace=True) as file:
		current_operator = None
		current_function = None
		for line in file:
			current_function = updateCurrentFunction(line, current_function)
			current_operator = updateCurrentOperator(line, current_operator)
			
			assigned_dag = "None"
			if current_operator in workflow_nodes:
				assigned_dag = "dag"

			operator_needed_dest = []
			if current_function is not None:
				for x in macro_mapping['destination']:
					if (op_func_mapping[current_function] in destinations[x]['required_operators']) or current_function == 'createDestination':
						operator_needed_dest.append(x)


			new_line = (line
				.replace("[OBJECT]", macro_mapping['source'])
				.replace("[DAG_COMPRESSION]", macro_mapping['compression'])
				.replace("[DAG_DEST]", "[\"" + "\",\"".join(operator_needed_dest) + "\"]")
				.replace("[DAG_DEST_SCHEMA]", macro_mapping['schema'])
				.replace("[DAG_DEST_TABLE]", macro_mapping['table'])
				.replace("[DAG_DEST_P_KEY]", macro_mapping['table_primary_key'])
				.replace("[DAG_DEST_S3]", macro_mapping['destination_s3'])
				.replace("[DAG_QUERY]", macro_mapping['query'].replace('"', '\\"'))
				.replace("[SOURCE_OBJECT]", macro_mapping['object'])
				.replace("[SOURCE_ENDPOINT]", macro_mapping['source_endpoint'])
				.replace("[DAG_NAME]", dag_name)
				.replace("[WORKFLOW]", final_workflow)
				.replace("[DAG_INSTANCE]", assigned_dag)
				.replace("[DESTINATION_ASSIGNMENT]", '\n\t'.join(dest_assignment))
				.replace("[OBJECT_INSTANCE VARIABLES]", '\n\t'.join(obj_instance_variables))
			)
			print(new_line, end="")