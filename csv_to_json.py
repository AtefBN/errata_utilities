import pandas as pd
import json
import re
import uuid
from b2handle.handleclient import EUDATHandleClient

# internal dependencies.
from constants import *


def extract_dataset_string_and_version(dataset_string):
    print(f'Now regex is analysing dataset {dataset_string}')
    try:
        search = re.search('^(.*)(\.v|#)(\d*)$', dataset_string)
        return search.group(1), search.group(3)
    except Exception as e:
        split_string = dataset_string.split('#')
        return split_string[0], split_string[1]


def replace_version_syntax_and_get_version(dataset_string):
    drs, version = extract_dataset_string_and_version(dataset_string)
    return drs+'#'+version, drs, version


def compute_pid_string(dataset_string, version):
    prefix = '21.14100/'
    hash_basis = str(dataset_string) + '.v' + str(version)
    hash_basis_utf8 = hash_basis.encode('utf-8')
    handle_string = uuid.uuid3(uuid.NAMESPACE_URL, str(hash_basis_utf8))
    handle_string = prefix + str(handle_string)

def retrieve_dataset_file_children(pid_string):
    handle_client = EUDATHandleClient.instantiate_for_read_access()
    encoded_dict = handle_client.retrieve_handle_record(handle_string)
    if encoded_dict is not None:
        handle_record = {k.decode('utf8'): v.decode('utf8') for k, v in encoded_dict.items()}
        handle_record
    else:
        print('ERROR {}'.format(pid_string))


def translate_severity(severity):
    if severity == 'low':
        return 'minor'
    else:
        return 'major'

counter = 0
# loading csv file into memory
csv_file = pd.read_csv(INPUT_CSV)
csv_reference_file = pd.read_csv(REFERENCE_CSV)
cross_dict = {}
for index, row in csv_reference_file.iterrows():
    dataset_string = row.to_list()[0].strip()
    pid_string = row.to_list()[1].strip()
    dataset_string, drs, version = replace_version_syntax_and_get_version(dataset_string)
    cross_dict[pid_string] = {}
    cross_dict[pid_string]['dset_id'] = drs
    cross_dict[pid_string]['dataset_qc'] = {}
    cross_dict[pid_string]['files'] = {}
    print(f'Now processing dataset {dataset_string}')
    specific_row = csv_file.loc[csv_file['resource_location'] == dataset_string]
    if specific_row.empty:
        print(f'Dataset {dataset_string} is safe.')
        cross_dict[pid_string]['dataset_qc']['qc_status'] = 'pass'
        cross_dict[pid_string]['dataset_qc']['error_severity'] = ''
        cross_dict[pid_string]['dataset_qc']['error_message'] = ''
    else:
        print(f'Found {dataset_string} in errata map.')
        cross_dict[pid_string]['dataset_qc']['qc_status'] = 'fail'
        cross_dict[pid_string]['dataset_qc']['error_severity'] = translate_severity(specific_row["severity"].values[0])
        cross_dict[pid_string]['dataset_qc']['error_message'] = specific_row["description"].values[0]
with open(CROSS_JSON_TEST, 'w') as f:
    json.dump(cross_dict, f)

print('TASK PERFORMED SUCCESSFULLY.')
