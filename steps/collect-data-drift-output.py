
import argparse
from azureml.core import Workspace, Dataset, Datastore, Run
from azureml.core.run import _OfflineRun
from azureml.data.dataset_factory import DataType

DATASTORE_NAME = 'workspaceblobstore'
FILE_DATASET_NAME = 'datadrift_file_results'
json_file_path = f'datadrift/metrics/**/output_*.json'


def parse_args():
    parser = argparse.ArgumentParser()
    #parser.add_argument('--output', dest='output', required=True)
    #parser.add_argument('--datadir', dest='datadir', required=True)

    return parser.parse_args()


args = parse_args()
print(f'Arguments: {args.__dict__}')


run = Run.get_context()
ws = Workspace.from_config() if type(run) == _OfflineRun else run.experiment.workspace

# Crate FileDataSet based on datadrift metrics which are saved in datastore as json files
dstore = Datastore.get(ws, DATASTORE_NAME)
print(f'dstore: {dstore}')
file_dataset = Dataset.File.from_files(path=(dstore,json_file_path))
print(f'file_dataset: {file_dataset}')
file_dataset.register(ws, FILE_DATASET_NAME, create_new_version=True)

#TODO: 
## add filter dataset
## add arguments instead of constants
