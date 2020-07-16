import os
import json
from constants import *

# Create dict with errata_id keys and dataset_id list as value
errata_map = {}
for item in os.listdir(DW_REPO):
    if '.txt' in item:
        errata_id = item[5:-4]
        dataset_list = []
        with open(os.path.join(DW_REPO, item)) as f:
            for line in f.readlines():
                dataset_list.append(line.rstrip())
            print(f'Added {len(dataset_list)} lines list for issue: {errata_id}')
        with open(os.path.join(DW_REPO, 'issue_'+errata_id+'.json')) as issue_json:
            issue = json.load(issue_json)

        for dataset_id in dataset_list:
            dataset_id = dataset_id.split('#')
            dataset_string = dataset_id[0]
            version = dataset_id[1]
            if dataset_string in errata_map.keys():
                errata_map[dataset_string].append({'issue_id': errata_id, 'version': version,
                                                   'severity': issue[SEVERITY], 'status': issue[STATUS]})
            else:
                errata_map[dataset_string] = [{'issue_id': errata_id, 'version': version,
                                               'severity': issue[SEVERITY], 'status': issue[STATUS]}]


with open(TARGET_JSON, 'w') as fp:
    json.dump(errata_map, fp)

print(errata_map)
