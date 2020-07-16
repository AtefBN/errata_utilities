import pandas as pd
import json
import re
# internal dependencies.
from constants import *


def extract_dataset_string_and_version(dataset_string):
    search = re.search('^(.*)(\.v)(\d*)$', dataset_string)
    return search.group(1), search.group(3)

cross_dict = {}
counter = 0
# loading csv file into memory
csv_file = pd.read_csv('datasets.csv')

with open(TARGET_JSON, 'r') as f:
    errata_dict = json.load(f)
    for index, row in csv_file.iterrows():
        print(f'now going through row {row.to_list()}')
        dataset_identifier_string = row.to_list()[0]
        print(dataset_identifier_string)
        dataset_string, version = extract_dataset_string_and_version(dataset_identifier_string)
        print('done.')
        pid_string = row.to_list()[1]
        #intializing cross referenced dictionary
        cross_dict[pid_string]={'dset_id': dataset_identifier_string, "qc_status":'pass'}
        if dataset_string in errata_dict.keys():
            errata = errata_dict[dataset_string]
            for erratum in errata:
                if erratum[VERSION] == version:
                    cross_dict[pid_string]['qc_status'] = 'ERROR'

with open(CROSS_JSON_TEST, 'w') as f:
    json.dump(cross_dict, f)

print('TASK PERFORMED SUCCESSFULLY.')
