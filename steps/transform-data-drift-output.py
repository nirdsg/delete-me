
import argparse
import json
import bigjson
import os
import utils
from azureml.core import Workspace, Dataset, Datastore, Run
from azureml.core.run import _OfflineRun

TEMP_DIRECTORY = 'temp'

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-data", type=str, dest='raw_dataset_id', help='raw dataset')
    parser.add_argument('--transformed-data', type=str, dest='transformed_data', default='transformed_data', help='Folder for results')
    return parser.parse_args()

args = parse_args()
print(f'Arguments: {args.__dict__}')
save_folder = args.transformed_data
os.makedirs(save_folder, exist_ok=True)

# Get the experiment run context
run = Run.get_context()
file_dataset = run.input_datasets['raw_data']
print(file_dataset)

with utils.temp_directory(TEMP_DIRECTORY):
    # Download json files defined by the dataset to temp directory
    json_file_paths = file_dataset.download(f'{TEMP_DIRECTORY}', overwrite=True)

    # Convert json files to jsonl files (in local directory) 
    for json_path in json_file_paths:
        
        # Read json file in streaming mode
        with open(json_path, 'rb') as f:
            json_data = bigjson.load(f)
            # Replace file name extension
            jsonl_path = os.path.splitext(json_path)[0]+'.jsonl'

            # Open jsonl file  
            with open(jsonl_path, 'w') as jsonl_file:
                # Iterates over input json
                for data in json_data:
                    # Converts json to a Python dict  
                    dict_data = data.to_python()
                    
                    # Saves the data to jsonl file
                    jsonl_file.write(json.dumps(dict_data)+"\n")
                    
        # Delete json file
        os.remove(json_path)

    # Upload jsonl files to datastore
    print("Saving Transformed Data...")
    print(os.listdir(f'{TEMP_DIRECTORY}'))
    output_dataset = Dataset.File.upload_directory(f'{TEMP_DIRECTORY}', target=save_folder)
    print(os.listdir(f'{save_folder}'))

print(os.listdir(f'{save_folder}'))

#TODO: 
## move to util
