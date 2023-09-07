import os
import json
import glob
from pathlib import Path

# set working directory to root dir
root_dir = Path(__file__).parent.parent
os.chdir(root_dir)

file_paths = glob.glob('**/**/test-config.json')

def remove_comments_from_json(file):
    with open(file) as f:
        lines = f.readlines()
        formatted_lines = []
        for line in lines:
            formatted_lines.append(line.split('/*')[0])
        return '\n'.join(formatted_lines)

print(f'Found {len(file_paths)} config files')
for file in file_paths:
    if os.path.exists(file):
        json_string_without_comments = remove_comments_from_json(file)
        try:
            json.loads(json_string_without_comments)
        except Exception as e:
            print(f'Encoutered json parsing error while reading {file}. Error: \n{e}')
            print(f'We had parsed:\n{json_string_without_comments}')
print('Finished parsing all test-config files.')
