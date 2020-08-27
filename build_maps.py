import pandas as pd
import os
import numpy as np


def create_measurement_map(xlsx_filepath):
    # measurement map (for Mengjing)
    measurement_dict = {}
    data = pd.read_excel(xlsx_filepath, sheet_name='allowed_measurements')
    for i in range(len(data)):
        spec_meas = data['Specific Measurement Type'][i]
        gen_meas = data['General Measurement Type'][i]
        measurement_dict[spec_meas] = gen_meas
    return measurement_dict


def create_disallowed_columns_plot(xlsx_filepath):
    # disallowed tuple (for both of us)
    disallowed_list = []
    data = pd.read_excel(xlsx_filepath,
                         sheet_name='disallowed_measurements_plot')
    for i in range(len(data)):
        elt = data['Measurement'][i]
        disallowed_list.append(elt)
    return disallowed_list


def create_disallowed_columns_json(xlsx_filepath):
    disallowed_list = []
    data = pd.read_excel(xlsx_filepath,
                         sheet_name='disallowed_measurements_json')
    for i in range(len(data)):
        elt = data['Measurement'][i]
        disallowed_list.append(elt)
    return disallowed_list

# for testing only
# filename = 'measurement_map.xlsx'
# print(create_dicts(filename))