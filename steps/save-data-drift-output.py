
import argparse
from azureml.core import Dataset, Datastore, Run
from azureml.core.run import _OfflineRun
from azureml.data.dataset_factory import DataType

PARTITION_FORMAT = '{DATADRIFT_ID}/{PARTITION_DATE:yyyy/MM/dd}/output_{RUN_ID}.json'


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--transformed-data", type=str, dest='transformed_data', help='transformed data')
    return parser.parse_args()

args = parse_args()
print(f'Arguments: {args.__dict__}')
transformed_data = args.transformed_data
print(transformed_data)


# Crate TabularDataSet based on converted jsonl files
output_dataset = Dataset.Tabular.from_json_lines_files(path=transformed_data, partition_format=PARTITION_FORMAT)
output_dataset = output_dataset.register_on_complete(name='datadrift_results_pipeline', description = 'datadrift results pipeline')
