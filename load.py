import datetime
import os
import time
from datetime import timedelta
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import sqlite3
import glob

from pandas import read_csv, read_excel
from build_maps import create_measurement_map, create_disallowed_columns_json

from galvani.galvani.BioLogic import MPRfile
from read_hpf import write_info_and_csv_from_hpf
from read_dat import read_dat
# from nptdms import TdmsFile
from npTDMS.nptdms.tdms import TdmsFile
from galvani.galvani import res2sqlite
from influxdb import InfluxDBClient

# TODO: switch everything to abspath

arbin_path = 'data_file_examples/arbin/'
biologic_path = 'data_file_examples/biologic/'
quickdaq_path = 'data_file_examples/quickdaq/'
tdms_path = 'data_file_examples/tdms/'


def parse_filename(filepath):
    '''parse filename'''
    # temp=filepath.split('/')
    # fname = temp[-1]
    # file_name, extension = fname.split('.')
    contents = filepath.split('/')
    filename = contents[-1]
    name, extension = filename.split('.')
    instrument_type = ''
    cell_num = ''
    test_num = ''
    if extension == 'res':
        instrument_type = 'Arbin'
    elif extension == 'mpr':
        instrument_type = 'BioLogic'
    elif extension == 'hpf':
        instrument_type = 'QuickDAQ'
    elif extension == 'tdms':
        instrument_type = 'TDMS'
    elif extension == 'dat':
        instrument_type = 'itest'
    if extension == 'res' or extension == 'tdms' or extension == 'dat':
        cell_num, test_num = name.split('_')
    elif extension == 'mpr':
        cell_num, test_num, dummy = name.split('_')
    elif extension == 'hpf':
        dummy, cell_num, test_num = name.split('_')
    test_num = test_num[4:]
    return extension, filename, instrument_type, cell_num, test_num


def get_res_obj(filepath):
    first_part, ext = filepath.split('.')
    outfilepath = first_part + '.s3db'
    res2sqlite.convert_arbin_to_sqlite(filepath, outfilepath)
    connection = sqlite3.connect(outfilepath)
    return connection


def get_res_columns(connection):
    columns = []
    cursor = connection.execute(
        """ PRAGMA table_info(Channel_Normal_Table) """)
    cols = cursor.fetchall()
    for elt in cols:
        columns.append(elt[1])
    return columns


def get_res_data(connection):
    cursor = connection.execute(""" SELECT * FROM Channel_Normal_Table """)
    return cursor.fetchall()


def get_mpr_obj(filepath):
    biologic_obj = MPRfile(filepath)
    print(biologic_obj.timestamp.strftime("%Y-%m-%dT%H:%M:%SZ"))
    return biologic_obj


get_mpr_obj('data_file_examples/biologic/UM26_Test001E_CA8.mpr')


def get_mpr_columns(biologic_obj):
    return list(biologic_obj.dtype.names)


def get_mpr_data(biologic_obj):
    return biologic_obj.data


def parse_hpf(filepath):
    '''read .hpf'''
    data = write_info_and_csv_from_hpf(filepath)
    return data
    # print(data)


def get_hpf_obj(filepath):
    pass


def get_hpf_cols(obj):
    pass


def get_hpf_data(obj):
    pass


def get_tdms_obj(filepath):
    tdms_file = TdmsFile.read(filepath)
    groups = tdms_file.groups()
    active_group = tdms_file[groups[0].name]
    channels = active_group.channels()
    active_channel = active_group[channels[0].name]
    return active_channel


def get_tdms_columns(active_channel):
    columns = ['Time (s)']
    columns.append(active_channel.properties['unit_string'])
    return columns


def get_tdms_data(active_channel):
    return active_channel.time_track(absolute_time=True), active_channel[:]


def get_dat_obj(filepath):
    return read_dat(filepath)


def get_dat_columns(obj):
    return obj.columns[5:]


# def get_data_obj(filepath):
#     """Get the data object"""
#     _, _, extension = get_file_details(filepath)
#     if extension == 'res':
#         first_part, ext = filepath.split('.')
#         outfilepath = first_part + '.s3db'
#         res2sqlite.convert_arbin_to_sqlite(filepath, outfilepath)
#         connection = sqlite3.connect(outfilepath)
#         return connection
#     elif extension == 'mpr':
#         biologic_obj = MPRfile(filepath)
#         return biologic_obj
#     elif extension == 'hpf':
#         pass
#     elif extension == 'tdms':
#         tdms_file = TdmsFile.read(filepath)
#         groups = tdms_file.groups()
#         active_group = tdms_file[groups[0].name]
#         channels = active_group.channels()
#         active_channel = active_group[channels[0].name]
#         return active_channel
#     else:
#         raise NotImplementedError('Haven\'t implemented this type of file yet')

# def get_column_names(data_obj, extension):
#     '''return all column name in a specific type of file'''
#     columns = []
#     if extension == 'res':
#         cursor = data_obj.execute(""" PRAGMA table_info(Channel_Normal_Table) """)
#         cols = cursor.fetchall()
#         for elt in cols:
#             columns.append(elt[1])
#     elif extension == 'mpr':
#         columns = list(data_obj.dtype.names)
#     elif extension == 'hpf':
#         columns = ['Temperature/°C', 'Voltage(V)', 'Current/A']
#     elif extension == 'tdms':
#         columns.append(data_obj.properties['unit_string'])
#     else:
#         raise NotImplementedError('Haven\'t implemented this type of file yet')
#     return columns

# def get_raw_data(data_obj, extension):
#     '''get all the data from a specific file'''
#     if extension == 'res':
#         cursor = data_obj.execute(""" SELECT * FROM Channel_Normal_Table """)
#         return cursor.fetchall()
#     elif extension == 'mpr':
#         return data_obj.data
#     elif extension == 'hpf':
#         return None
#     elif extension == 'tdms':
#         data = []
#         data.append(data_obj.time_track())
#         data.append(data_obj[:])
#         return data
#     else:
#         raise NotImplementedError('Haven\'t implemented this type of file yet')


# def parse_tdms(filepath):
#     '''read .tdms'''
#     # filepath --> full tdms filepath from ~
#     tdms_file = TdmsFile.read(filepath)
#     groups = tdms_file.groups()
#     active_group = tdms_file[groups[0].name]
#     channels = active_group.channels()
#     active_channel = active_group[channels[0].name]
#     return active_channel

# def parse_mpr(filepath):
#     '''read .mpr'''
#     # filepath --> full mpr filepath from ~
#     mpr_file = MPRfile(filepath)
#     return mpr_file

# def read_res(fname):
#     '''read res'''
#     file_name, dummy = fname.split('.')
#     res2sqlite.convert_arbin_to_sqlite(
#         arbin_path + fname,
#         arbin_path + file_name + '.s3db')
#     connection = sqlite3.connect(arbin_path + file_name + '.s3db')
#     cur = connection.execute('SELECT * FROM Channel_Normal_Table')
#     data = cur.fetchall()
#     # print('********************************')
#     # print(data)
#     # print('********************************')


def unit_conversion(unit):
    '''implement unit correction'''
    if unit == 'mA':
        return 0.001
    elif unit == 'mm':
        return 0.001
    elif unit == 'V/mA':
        return 1000
    elif unit == 'mA.h':
        return 0.001
    elif unit == 'µF':
        return 0.000001
    else:
        return 1


def store_timestamp(file_name, timestamp, filepath):
    file, ext = file_name.split('.')
    df = read_excel(filepath)
    if file not in df.columns:
        df[file] = timestamp
    df.to_excel("path to save")


def get_res_timestamp(file_name, filepath):
    file, ext = file_name.split('.')
    df = read_excel(filepath)
    try:
        return df[file]
    except KeyError:
        raise KeyError(
            'No corresponding tdms file.'
        )


def construct_json(extension, file_name, instrument_type, cell_num, test_num,
                   test_description, filepath):
    '''create json'''
    # file_name: UM34_Test005E, filepath: .../data_file_examples/arbin/
    xlsx_filepath = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                                 'measurement_map.xlsx')
    time_filepath = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                                 'timestamp.xlsx')
    # print(xlsx_filepath)
    map_measurement_type = {
        # TODO: remove unnecessary columns, replace with measurement_map dict
        'control/V': 'voltage [V]',
        'Ecell/V': 'voltage [V]',
        'Ewe/V': 'voltage [V]',
        'Voltage(V)': 'voltage [V]',
        'Voltage': 'voltage [V]',
        'Temperature /°C': 'temperature [C]',
        'Temperature/°C': 'temperature [C]',
        'control/mA': 'current [A]',
        'I/mA': 'current [A]',
        'Current/A': 'current [A]',
        'Current': 'current [A]',
        'I Range': 'current [A]',
        'displacement': 'displacement [m]',
        'control/V/mA': 'resistance [Ohm]',
        'R/Ohm': 'resistance [Ohm]',
        'Internal_Resistance(Ohm)': 'resistance [Ohm]',
        'Internal_Resistance': 'resistance [Ohm]',
        'P/W': 'power [W]',
        'cycle number': 'cycle number',
        'Cycle_Index': 'cycle number',
        'dQ/mA.h': 'dQ / Q-Qo [Ah]',
        '(Q-Qo)/mA.h': 'dQ / Q-Qo [Ah]',
        'Energy/W.h': 'energy [Wh]',
        'Energy charge/W.h': 'energy_charge [Wh]',
        'Charge_Energy(Wh)': 'energy_charge [Wh]',
        'Charge_Energy': 'energy_charge [Wh]',
        'Energy discharge/W.h': 'energy_discharge [Wh]',
        'Discharge_Energy(Wh)': 'energy_discharge [Wh]',
        'Discharge_Energy': 'energy_discharge [Wh]',
        'Q charge/discharge/mA.h': 'Q charge/discharge [Ah]',
        'half cycle': 'half cycle',
        'Capacitance charge/µF': 'capacitance_charge [F]',
        'Capacitance discharge/µF': 'capacitance_discharge [F]',
        'Q charge/mA.h': 'Q_charge [Ah]',
        'Q discharge/mA.h': 'Q_discharge [Ah]',
        'Capacity/mA.h': 'capacity [Ah]',
        'Charge_Capacity(Ah)': 'capacity [Ah]',
        'Charge_Capacity': 'capacity [Ah]',
        'Discharge_Capacity(Ah)': 'capacity [Ah]',
        'Discharge_Capacity': 'capacity [Ah]',
        'Efficiency/%': 'efficiency [%]',
        '|Z|/Ohm': 'impedance [Ohm]',
        'Re(Z)/Ohm': 'impedance [Ohm]',
        '-Im(Z)/Ohm': 'impedance [Ohm]',
        'AC_Impedance(Ohm)': 'impedance [Ohm]',
        'AC_Impedance': 'impedance [Ohm]',
        'Phase(Z)/deg': 'phase_angle [deg]',
        'Phase(Y)/deg': 'phase_angle [deg]',
        'ACI_Phase_Angle(Deg)': 'phase_angle [deg]',
        'ACI_Phase_Angle': 'phase_angle [deg]',
        'freq/Hz': 'frequency [Hz]',
        '|Y|/Ohm-1': 'admittance',
        'Re(Y)/Ohm-1': 'admittance',
        'Im(Y)/Ohm-1': 'admittance',
        '"dV/dt"': 'dV/dt [V/s]',
        'dV/dt': 'dV/dt [V/s]',
        'z cycle': 'z cycle',
    }

    bad_cols_backend = create_disallowed_columns_json(xlsx_filepath)

    # print(bad_cols_backend)

    data_point = []
    if extension == 'res':
        connection = get_res_obj(filepath)
        columns = get_res_columns(connection)
        data = get_res_data(connection)
        # print(columns)
        # cur = connection.execute(
        #     'SELECT Test_Time, Cycle_Index,Current,Voltage, Charge_Capacity, Discharge_Capacity, Charge_Energy, Discharge_Energy, "dV/dt", Internal_Resistance, AC_Impedance, ACI_Phase_Angle ' +
        #     'FROM Channel_Normal_Table'
        # )
        # data = cur.fetchall()
        # cur = connection.execute('SELECT Start_DateTime FROM Global_table')
        # start_time = cur.fetchall()[0][0]
        # print(start_time)
        # dateArray = datetime.datetime.utcfromtimestamp(int(start_time))
        # accurate_time = datetime.datetime.strptime('2020/3/6 12:54:07',
        #                                           '%Y/%m/%d %H:%M:%S')
        # diff = accurate_time - datetime.datetime.strptime(
        #    '1970/01/01 12:11:36',
        #    '%Y/%m/%d %H:%M:%S')
        # dateArray = dateArray + diff
        # print(dateArray)
        # print(data[2][0])
        timestamps = get_res_timestamp(file_name, time_filepath)
        for index in range(len(data)):
            d = data[index]
            timestamp = timestamps[index]
            for i in range(1, len(d)):
                if columns[i] not in bad_cols_backend:
                    specific_measurement_type = columns[i]
                    general_measurement_type = map_measurement_type[
                        specific_measurement_type]
                    value = d[i]
                    data_point.append(
                        {
                            "measurement": cell_num,
                            "tags": {
                                "original_file_name": file_name,
                                "test_number": test_num,
                                "instrument_type": instrument_type,
                                "test_description": test_description,
                                "general_measurement_type": general_measurement_type,
                                "specific_measurement_type": specific_measurement_type
                            },
                            "timestamp": timestamp,
                            "fields": {
                                "value": value
                            }
                        }
                    )
        # print(data_point[0])
        # print(data_point[100])
        # print(data_point[20])
    elif extension == 'mpr':
        biologic_obj = get_mpr_obj(filepath)
        start_time = biologic_obj.timestamp
        # print('**********')
        # print(start_time)
        # print('**********')
        columns = get_mpr_columns(biologic_obj)
        print(columns)
        # print(columns)
        data = get_mpr_data(biologic_obj)
        for d in data:
            timestamp = start_time.strftime("%Y-%m-%dT%H:%M:%SZ")
            for i in range(len(columns)):
                col = columns[i]
                if col == 'time/s':
                    timestamp = (
                            start_time + timedelta(seconds=d[i])).strftime(
                        "%Y-%m-%dT%H:%M:%SZ")
                elif col not in bad_cols_backend:
                    if col == 'I Range':
                        unit = 'mA'
                    elif col == 'half cycle':
                        unit = ''
                    elif col == 'x':
                        unit = ''
                    else:
                        _, unit = col.split('/', 1)
                    factor = unit_conversion(unit)
                    specific_measurement_type = col
                    general_measurement_type = map_measurement_type[
                        specific_measurement_type]
                    value = d[i]
                    data_point.append(
                        {
                            "measurement": cell_num,
                            "tags": {
                                "original_file_name": file_name,
                                "test_number": test_num,
                                "instrument_type": instrument_type,
                                "test_description": test_description,
                                "general_measurement_type": general_measurement_type,
                                "specific_measurement_type": specific_measurement_type
                            },
                            "timestamp": timestamp,
                            "fields": {
                                "value": value * factor
                            }
                        }
                    )
        # print(data_point[0])
    # elif extension == 'hpf':
    #     time_dict = read_hpf(file)
    #     # print('****************')
    #     # print(time_dict['V'][0])
    #     # print('****************')
    #     for name in glob.glob(
    #             quickdaq_path + file_name + '_data_channel*.csv'):
    #         f = open(name)
    #         unit = f.readline().strip()
    #         # print(unit)
    #         specific_measurement_type = ''
    #         time_list = []
    #         if (unit == 'Deg C'):
    #             specific_measurement_type = 'Temperature/°C'
    #             time_list = time_dict['Deg C']
    #         elif (unit == 'V'):
    #             specific_measurement_type = 'Voltage(V)'
    #             time_list = time_dict['V']
    #         elif (unit == 'Amps'):
    #             specific_measurement_type = 'Current/A'
    #             time_list = time_dict['Amps']
    #         t = time_list[0][:-1]
    #         start_time = datetime.datetime.strptime(t, "%Y/%m/%d %H:%M:%S.%f")
    #         general_measurement_type = map_measurement_type[
    #             specific_measurement_type]
    #         lines = f.readlines()
    #         # lines = lines[1:]
    #         i = 0
    #         for value in lines:
    #             timestamp = i * time_list[1]
    #             i += 1
    #             data_point.append(
    #                 {
    #                     "measurement": cell_num,
    #                     "tags": {
    #                         "original_file_name": file_name,
    #                         "test_number": test_num,
    #                         "instrument_type": instrument_type,
    #                         "test_description": test_description,
    #                         "general_measurement_type": general_measurement_type,
    #                         "specific_measurement_type": specific_measurement_type
    #                     },
    #                     "timestamp":
    #                         (start_time + timedelta(
    #                             seconds=timestamp)).strftime(
    #                             "%Y-%m-%dT%H:%M:%S.%fZ"),
    #                     "fields": {
    #                         "value": value.strip()
    #                     }
    #                 }
    #             )
    #     # print(data_point[2])
    elif extension == 'tdms':
        active_channel = get_tdms_obj(filepath)
        # columns = get_tdms_columns(active_channel)
        time, data = get_tdms_data(active_channel)
        # print(time, data)
        # f = open(tdms_path + file_name + '_disp.csv')
        specific_measurement_type = 'displacement'
        general_measurement_type = map_measurement_type[
            specific_measurement_type]
        # timestamp = ''
        for i in range(len(data)):
            data_point.append(
                {
                    "measurement": cell_num,
                    "tags": {
                        "original_file_name": file_name,
                        "test_number": test_num,
                        "instrument_type": instrument_type,
                        "test_description": test_description,
                        "general_measurement_type": general_measurement_type,
                        "specific_measurement_type": specific_measurement_type
                    },
                    "timestamp": str(time[i]) + 'Z',
                    "fields": {
                        "value": float(data[i]) / 1000  # shouldn't this be 1E6
                    }
                }
            )
        # print(data_point[1])
    elif extension == 'dat':
        dat_obj = get_dat_obj(filepath)
        columns = get_dat_columns(dat_obj)
        timestamps = []
        for i in range(1, len(dat_obj['Date'])):
            temp_time = dat_obj['Date'][i] + ' ' + dat_obj['Time'][:8] + \
                        dat_obj['Time'][9:]
            date = datetime.datetime.strptime(temp_time,
                                              '%Y/%m/%d %H:%M:%S.%f')
            timestamps.append(date.strftime("%Y-%m-%dT%H:%M:%S.%fZ"))
        for col in columns[2:]:
            switcher = {
                'Voltage1': "/V",
                'Voltage2': "/V",
                'Voltage3': "/V",
                'Voltage4': "/V",
                'Pack Current FB': "/A",
                'Pack Current SP': "/A",
                'Current': "/A",
                'I_SP': "/A",
                'numCctControl': "",
                'Pack Cycler Mode': "",
                'qCct': "/Ah",
                'Thermocouple1': "/°C",
                'Thermocouple2': "/°C",
                'Thermocouple3': "/°C",
                'Thermocouple4': "/°C",
                'Thermocouple5': "/°C",
                'Chamber Temp.': "/°C",
                'Percent OUT': "/%",
                'tEcc1ProdFeedback': "/°C",
                'tEcc1ProdSetpoint': "/°C",
                'tEcc1SetpointIn': "/°C",
                'Pack Voltage FB': "/V",
                'Pack Voltage SP': "/V",
                'Voltage': "/V",
                'V_SP': "/V",
                'AmpHrs': "/Ah",
                'qahCell1': "/Ah",
                'numEcc1Status': ""
            }
            unit = switcher.get(col, "")
            specific_measurement_type = col + unit
            general_measurement_type = map_measurement_type[
                specific_measurement_type]
            for i in range(1, len(dat_obj['Date'])):
                data_point.append(
                    {
                        "measurement": cell_num,
                        "tags": {
                            "original_file_name": file_name,
                            "test_number": test_num,
                            "instrument_type": instrument_type,
                            "test_description": test_description,
                            "general_measurement_type": general_measurement_type,
                            "specific_measurement_type": specific_measurement_type
                        },
                        "timestamp": timestamps[i],
                        "fields": {
                            "value": float(dat_obj[col][i])
                        }
                    }
                )
        print(data_point[1])
    else:
        raise NotImplementedError('have not implemented this kind of file yet')
    return data_point

# def main():
# res_test = [
#     'res',
#     'UM34_Test005E.res',
#     'Arbin',
#     'UM34',
#     '005E',
#     'idk what',
#     'data_file_examples/arbin/UM34_Test005E.res'
# ]
# mpr_test = [
#     'mpr',
#     'UM26_Test001E_CA8.mpr',
#     'Biologic',
#     'UM26',
#     '001E',
#     'idk what',
#     'data_file_examples/biologic/UM26_Test001E_CA8.mpr'
# ]
# hpf_test = [
#     'hpf',
#     'QuickDAQ_UM34_Test005E.hpf',
#     'Quickdaq',
#     'UM34',
#     '005E',
#     'idk what',
#     'data_file_examples/quickdaq/QuickDAQF_UM34_Test005E.hpf'
# ]
# tdms_test = [
#     'tdms',
#     'UM26_Test001E.tdms',
#     'TDMS',
#     'UM26',
#     '001E',
#     'idk what',
#     'data_file_examples/tdms/UM26_Test001E.tdms'
# ]

# json_body = construct_json(tdms_test[0], tdms_test[1], tdms_test[2], tdms_test[3], tdms_test[4], tdms_test[5], tdms_test[6])
# print(json_body)

#     hpf_filepath = os.path.join(os.path.abspath(os.path.dirname(__file__)), 'data_file_examples/quickdaq/QuickDAQF_UM34_Test005E.hpf')
#     print(parse_hpf(hpf_filepath))

# if __name__ == '__main__':
#     main()

# construct_json('mpr','UM26_Test001E', 'biologic', 'UM26', '001E', '', 'data_file_examples/biologic/UM26_Test001E_CA8.mpr')
# read_dat('data_file_examples/itest/BatteryLab_D3_M8_Y2016_T17_34_17.dat')
