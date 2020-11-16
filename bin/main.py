

# IMPORTS ============================= #
import os
import pandas as pd
import datetime
import json
import copy
import time
# SET DIRECTORY ======================= #

# CONFIG FILE ========================= #
config_json = pd.read_json('lookup_config.json')
config_columns = list(config_json.columns)

config_dat = config_json.files_aggregator.files
config_df = pd.DataFrame(config_dat)
config_df.filename = config_df.filename.apply(lambda x: x.split('../')[-1])

# BUILD DICTIONARY ==================== #
timenow = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
config_file = {}

config_base = {
    'update_time': timenow,
    'batch_name': config_json.batch_name[0],
    'state': config_json.state[0],
    'county': config_json.county[0],
    'raw_data': {}
}

for file in config_df.filename.tolist():
    print(f'loading records from {file}')
    df = pd.read_csv('../'+file, dtype='str')
    df = df.where(pd.notnull(df), None)
    df = df[:1001]  # uncomment for short tests
    df_fields = df.columns.tolist()
    config_index = config_df.filename.tolist().index(file)
    file_field = config_df.lable[config_index]
    config_single = config_df.loc[config_index].single_record_per_index  # boolean
    for i, row in df.iterrows():
        quick_ref = row.QuickRefID
        if i % 1000 == 0:
            print(f'Row {i} processed...')
        for field in df_fields:
            # handle non-single case
            if not config_single:
                if quick_ref in config_file.keys():
                    if file_field in config_file[quick_ref].keys():
                        if istate == i:  # ignore warning, must be defined at least once on 64 or 69
                            config_file[quick_ref][file_field][-1][field] = row[field]
                        else:
                            config_file[quick_ref][file_field].append({field: row[field]})
                            istate = copy.deepcopy(i)
                    else:
                        config_file[quick_ref][file_field] = [{field: row[field]}]
                        istate = copy.deepcopy(i)
                else:
                    config_file[quick_ref] = {}
                    config_file[quick_ref][file_field] = [{field: row[field]}]
                    istate = copy.deepcopy(i)

            elif quick_ref in config_file.keys():
                if file_field in config_file[quick_ref].keys():
                    config_file[quick_ref][file_field][field] = row[field]
                else:
                    config_file[quick_ref][file_field] = {}
                    config_file[quick_ref][file_field][field] = row[field]
            else:
                config_file[quick_ref] = {}
                config_file[quick_ref][file_field] = {}
                config_file[quick_ref][file_field][field] = row[field]


# transform dictionary into list of dictionaries with proper key structure
config_set = [config_base.copy() for key in config_file.keys()]
config_keys = list(config_file.keys())

for x, key in enumerate(config_keys):
    config_set[x]['raw_data'] = config_file[key]

timenow = datetime.datetime.now().strftime('%Y-%m-%dT%H-%M-%S')
output_filename = '../output/Result_'+str(timenow)+'.json'
with open(output_filename, 'w') as outputfile:
    outputfile.write(json.dump(config_set,f ,indent=4))


