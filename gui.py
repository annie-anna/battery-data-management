# FIX TKINTER ISSUE WITH XMING SERVER - FIXED!
# https://stackoverflow.com/questions/48254530/tkinter-in-ubuntu-inside-windows-10-error-no-display-name-and-no-display-env
# SHOULD CONSIDER BINDING INFLUX CLIENT TO FRONT-END
# FILEPATH ERROR: CLICKING UPLOAD AND THEN IMMEDIATELY EXITING DESTROYS FILEPATH INFORMATION
# MAKE CODE MORE MODULAR/ABSTRACTION AND REDUCE DUPLICATION AFTER IT WORKS
# WHEN BUILDING DATA DICT AND POPULATING DROPDOWN MENU, JUST DO IT UNDER ONE ACTION, NOT TWO
# FOR EACH RETURNED ERROR, GIVE DIALOG BOX

# for each file type, create a dictionary
# where key = column name in file and
# value = tuple or list of all the data for that column

try:
    import tkinter as tk
except ImportError:
    import Tkinter as tk
from influxdb import InfluxDBClient
from influx_methods import *  # used for handling influx client setup
from load import *
from build_maps import create_disallowed_columns_plot
import galvani
from galvani import BioLogic as blg
from galvani import res2sqlite as res
import sqlite3
import nptdms
from nptdms import TdmsFile as tdms
import sys
import os
import numpy as np
import matplotlib
import matplotlib.pyplot as plt
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg, \
    NavigationToolbar2Tk

matplotlib.use('TKAgg')  # might need to change this based on OS?
from tkinter import filedialog

root = tk.Tk()
root.title('Database GUI')

# set globals
HEIGHT = 800  # height of app window
WIDTH = 1200  # width of app window

HOME_DIR = os.path.abspath(
    os.path.dirname(__file__))  # where gui script is located
BAD_COLS_FILE = os.path.join(HOME_DIR,
                             'measurement_map.xlsx')  # csv script which has measurement map & disallowed columns
BAD_COLS = create_disallowed_columns_plot(BAD_COLS_FILE)

X_OPTS = ['']  # list of options in x_dropdown
X_SELECT = tk.StringVar(root)  # selected option in x_dropdown
Y_OPTS = ['']  # list of options in y_dropdown
Y_SELECT = tk.StringVar(root)  # selected option in y_dropdown

DATA_DICT = {}  # key: column name (incl time), value: data vector for col
FILEPATH = []

# Application home
canvas = tk.Canvas(root, height=HEIGHT, width=WIDTH)
canvas.pack()

frame = tk.Frame(root, bg='#00274c')
frame.place(relx=0, rely=0, relwidth=1, relheight=1)


# create custom functions to handle events
def upload_file():
    """Uploads a file after you click button"""
    FILEPATH.clear()
    root.filename = filedialog.askopenfilename(initialdir=HOME_DIR,
                                               title="Select a data file...",
                                               filetypes=(('all files', '*.*'),
                                                          ('arbin files',
                                                           '*.res'), (
                                                          'biologic files',
                                                          '*.mpr'), (
                                                          'quickdaq files',
                                                          '*.hpf'), (
                                                          'tdms files',
                                                          '*.tdms')))
    FILEPATH.append(root.filename)
    # print(root.filename)
    return root.filename
    # TODO: if the filename isn't Peyman's format, then raise an error


def clear_entries():
    """Clears entries of entry boxes"""
    filename_entry.delete(0, tk.END)
    testnumber_entry.delete(0, tk.END)
    cellnumber_entry.delete(0, tk.END)
    instrtype_entry.delete(0, tk.END)
    filename_entry.insert(0, '')
    testnumber_entry.insert(0, '')
    cellnumber_entry.insert(0, '')
    instrtype_entry.insert(0, '')


def create_data_obj(filepath, extension):
    data_obj = []
    if extension == 'res':
        data_obj = get_res_obj(filepath)
    elif extension == 'mpr':
        data_obj = get_mpr_obj(filepath)
    elif extension == 'hpf':
        pass
    elif extension == 'tdms':
        data_obj = get_tdms_obj(filepath)
    else:
        raise NotImplementedError('have not implemented this file type yet')
    return data_obj


def prepopulate():
    """Prepopulates tag fields and dropdown options based on filename and builds data dict"""
    filepath = upload_file()
    if filepath != "" and type(filepath) != tuple:
        # populate tag fields
        clear_entries()  # include EVERYTHING you want to destroy in a file instance here
        DATA_DICT.clear()
        extension, filename, instrument_type, cell_num, test_num = parse_filename(
            filepath)
        filename_entry.insert(0, filename)
        testnumber_entry.insert(0, test_num)
        cellnumber_entry.insert(0, cell_num)
        instrtype_entry.insert(0, instrument_type)
        X_SELECT.set('')
        Y_SELECT.set('')
        x_dropdown['menu'].delete(0, 'end')
        y_dropdown['menu'].delete(0, 'end')

        # get data obj
        data_obj = create_data_obj(filepath, extension)

        # populate axis fields
        columns = []
        if extension == 'res':
            columns = get_res_columns(data_obj)
        elif extension == 'mpr':
            columns = get_mpr_columns(data_obj)
        elif extension == 'hpf':
            pass
        elif extension == 'tdms':
            columns = get_tdms_columns(data_obj)
        else:
            raise NotImplementedError(
                'have not implemented this file type yet')
        for elt in columns:
            if elt not in BAD_COLS:
                x_dropdown['menu'].add_command(label=elt,
                                               command=tk._setit(X_SELECT,
                                                                 elt))
                y_dropdown['menu'].add_command(label=elt,
                                               command=tk._setit(Y_SELECT,
                                                                 elt))
                DATA_DICT[elt] = []

        # create data dictionary
        data = []
        if extension == 'res':
            data = get_res_data(data_obj)
            for i in range(len(data)):
                for j in range(len(data[i])):
                    if columns[j] not in BAD_COLS:
                        DATA_DICT[columns[j]].append(data[i][j])
        elif extension == 'mpr':
            data = get_mpr_data(data_obj)
            for i in range(len(data)):
                for j in range(len(data[i])):
                    if columns[j] not in BAD_COLS:
                        DATA_DICT[columns[j]].append(data[i][j])
        elif extension == 'hpf':
            pass
        elif extension == 'tdms':
            time, data = get_tdms_data(data_obj)
            DATA_DICT[columns[0]] = time
            DATA_DICT[columns[1]] = data
    # print(FILEPATH)

    # if fext == 'res':
    #     infilepath = os.path.join('data_file_examples', 'arbin', fname)
    #     name, ext = fname.split('.')
    #     outfilepath = os.path.join('data_file_examples', 'arbin', name+'.s3db')
    #     res.convert_arbin_to_sqlite(infilepath, outfilepath)
    #     connection = sqlite3.connect(outfilepath)
    #     cursor = connection.execute(""" PRAGMA table_info(Channel_Normal_Table) """)
    #     columns = cursor.fetchall()
    #     # print(columns)
    #     timevar = columns[2][1]
    #     x_dropdown['menu'].add_command(label=timevar, command=tk._setit(X_SELECT, timevar))
    #     for tup in columns:
    #         if tup[1] not in BAD_COLS:
    #             y_dropdown['menu'].add_command(label=tup[1], command=tk._setit(Y_SELECT, tup[1]))
    # elif fext == 'mpr':
    #     infilepath = os.path.join('data_file_examples', 'biologic', fname)
    #     biologic_obj = blg.MPRfile(infilepath)
    #     columns = biologic_obj.dtype.names
    #     # print(columns)
    #     timevar = columns[3]
    #     x_dropdown['menu'].add_command(label=timevar, command=tk._setit(X_SELECT, timevar))
    #     for item in columns:
    #         if item not in BAD_COLS:
    #             y_dropdown['menu'].add_command(label=item, command=tk._setit(Y_SELECT, item))
    # elif fext == 'hpf':
    #     raise NotImplementedError('not implemented yet')  # include error popup
    # elif fext == 'tdms':
    #     infilepath = os.path.join('data_file_examples', 'tdms', fname)
    #     tdms_obj = tdms.read(infilepath)
    #     groups = tdms_obj.groups()
    #     active_group = tdms_obj[groups[0].name]
    #     channels = active_group.channels()
    #     active_channel = active_group[channels[0].name]
    #     # print(active_channel.properties['unit_string'])
    #     timevar = 'Time (s)'
    #     x_dropdown['menu'].add_command(label=timevar, command=tk._setit(X_SELECT, timevar))
    #     y_val = active_channel.properties['unit_string']
    #     y_dropdown['menu'].add_command(label=y_val, command=tk._setit(Y_SELECT, y_val))
    # else:
    #     raise NotImplementedError('Cannot support these file types')  # include error popup

    # build data dictionary for plotting
    # populate_data_dict(fname)


# def populate_data_dict(fname):
#     """Populates the data dictionary for single file"""
#     # fname example: UM26_Test001E.res
#     fn, fext = fname.split('.')
#     if fext == 'res':
#         infilepath = os.path.join('data_file_examples', 'arbin', fname)
#         outfilepath = os.path.join('data_file_examples', 'arbin', fn+'.s3db')
#         res.convert_arbin_to_sqlite(infilepath, outfilepath)
#         connection = sqlite3.connect(outfilepath)
#         cursor = connection.execute(""" PRAGMA table_info(Channel_Normal_Table) """)
#         columns = cursor.fetchall()
#         cursor = connection.execute(""" SELECT * FROM Channel_Normal_Table """)
#         data = cursor.fetchall()
#         for tup in columns:
#             if tup[1] not in BAD_COLS:
#                 DATA_DICT[tup[1]] = []
#         for i in range(len(data)):
#             for j in range(len(data[i])):
#                 if columns[j][1] not in BAD_COLS:
#                     DATA_DICT[columns[j][1]].append(data[i][j])
#     elif fext == 'mpr':
#         infilepath = os.path.join('data_file_examples', 'biologic', fname)
#         biologic_obj = blg.MPRfile(infilepath)
#         columns = biologic_obj.dtype.names
#         data = biologic_obj.data
#         for item in columns:
#             if item not in BAD_COLS:
#                 DATA_DICT[item] = []
#         for i in range(len(data)):
#             for j in range(len(data[i])):
#                 if columns[j] not in BAD_COLS:
#                     DATA_DICT[columns[j]].append(data[i][j])
#     elif fext == 'hpf':
#         pass
#     elif fext == 'tdms':
#         infilepath = os.path.join('data_file_examples', 'tdms', fname)
#         tdms_obj = tdms.read(infilepath)
#         groups = tdms_obj.groups()
#         active_group = tdms_obj[groups[0].name]
#         channels = active_group.channels()
#         active_channel = active_group[channels[0].name]
#         data = active_channel[:].tolist()
#         time = active_channel.time_track().tolist()
#         DATA_DICT['Time (s)'] = time
#         DATA_DICT[active_channel.properties['unit_string']] = data
#     else:
#         raise NotImplementedError('Cannot support these file types')  # include error popup

#     # for key, value in DATA_DICT.items():
#     #     print(len(DATA_DICT[key]))


def plot_data():
    """Plots the selected data"""
    if X_SELECT.get() != "" and Y_SELECT.get() != "":
        x_data = DATA_DICT[X_SELECT.get()]
        y_data = DATA_DICT[Y_SELECT.get()]
        # print(x_data, y_data)
        plot_name = Y_SELECT.get() + ' vs ' + X_SELECT.get()
        plt.figure(num=plot_name)  # put each graph on a new window
        plt.plot(x_data, y_data, 'o')
        plt.title(plot_name)
        plt.xlabel(X_SELECT.get())
        plt.ylabel(Y_SELECT.get())
        plt.show()


def submit_influxdb():  # TODO: FIGURE OUT WHAT BUTTON TYPES TO HAVE IN MSGBOX
    """Submits data to influxdb. Has some error checking"""
    # print(FILEPATH[0])
    msgbox = tk.messagebox.askquestion('Please Preview Data',
                                       'Please make sure the data plots make sense.\nAlso make sure all the information fields are filled in.\nDid you already do this?',
                                       icon='warning')
    if msgbox == 'yes':
        if filename_entry.get() == '' or testnumber_entry.get() == '' or cellnumber_entry.get() == '' or instrtype_entry.get() == '':
            tk.messagebox.showerror('Missing Fields',
                                    'Oops!\nIt seems like some of the information fields are missing.\nPlease fix this first!')
        else:
            # do we need something which displays the server, port number, and database options to the user?
            server_name = # TODO: ADD YOUR OWN SERVER_NAME
            port_num = 8086  # CHANGEABLE
            username = # TODO: ADD YOUR OWN USERNAME
            password = # TODO: ADD YOUR OWN PASSWORD
            db = # TODO: ADD YOUR OWN DB NAME
            msgbox = tk.messagebox.askquestion('Review Database Information',
                                               'Is the following database information correct?\nServer: {0}\nPort: {1}\nUsername: {2}\nDatabase: {3}'.format(
                                                   server_name, port_num,
                                                   username, db),
                                               icon='question')
            if msgbox == 'yes':
                client = establish_client(server_name, port_num, username,
                                          password)
                db_list = get_databases(client)
                db_dict = {'name': db}
                if db_dict not in db_list:
                    msgbox = tk.messagebox.askquestion(
                        'Couldn\'t Find Database',
                        'We could not find the \'{0}\' database. Would you like to create it?'.format(
                            db), icon='error')
                    if msgbox == 'yes':
                        create_db(client, db)
                else:
                    enter_db(client, db)
                    cell_number = cellnumber_entry.get()
                    test_number = testnumber_entry.get()
                    instr_type = instrtype_entry.get()
                    # also evaluate measurement types?
                    query = 'SELECT * FROM {0};'.format(cell_number)
                    # query = 'SELECT * FROM test_measurement;'
                    result = query_influx(client, query)
                    points = list(result.get_points(measurement=cell_number,
                                                    tags={
                                                        'test_number': test_number,
                                                        'instrument_type': instr_type}))
                    # points = list(result.get_points(measurement='test_measurement'))
                    # print(points)
                    if len(points) != 0:
                        msgbox = tk.messagebox.askquestion(
                            'Overlap Identified',
                            'Data for this cell number, test number, and instrument type already exist.\nAre you sure you want to proceed with uploading this data?',
                            icon='warning')
                        if msgbox == 'no':
                            return

                    # prepare the JSON body here? call json_constructor
                    file_name = filename_entry.get()
                    name, extension = file_name.split('.')
                    test_descr = testdescr_entry.get('1.0',
                                                     tk.END)  # may need to format this to be cleaner?
                    json_body = construct_json(extension, file_name,
                                               instr_type, cell_number,
                                               test_number, test_descr,
                                               FILEPATH[0])
                    # print(json_body)

                    msgbox = tk.messagebox.askquestion('Confirmation',
                                                       'Please click \'Yes\' to confirm that you want to submit this data to InfluxDB. Otherwise, click \'No\'',
                                                       icon='info')
                    if msgbox == 'yes':
                        # execute write points code here
                        try:
                            push_data(client,
                                      json_body)  # TODO: request could be too large apparently
                            msgbox = tk.messagebox.askquestion(
                                'Congratulations!',
                                'Congratulations! Your data was successfully uploaded to your InfluxDB database.\nWould you like to submit more data?\nIf you do, hit \'Yes\'. Otherwise, hit \'No\'',
                                icon='info')
                            if msgbox == 'no':
                                root.destroy()
                        except:
                            tk.messagebox.showerror('Oops!',
                                                    'Seems like there was an error with your submission.\nPlease try again!')


# APPLICATION BODY
main_label = tk.Label(frame, anchor=tk.CENTER,
                      text='Please check to see whether the pre-populated fields are correct. If not, please change them. Use the dropdown lists and Preview Plot functionalities to check the data.',
                      bg='#ffcb05', fg='black', font='Helvetica 12 bold')
main_label.place(relx=0.02, rely=0.02, relwidth=0.96, relheight=0.1)

filename_label = tk.Label(frame, anchor=tk.CENTER, text='File Name',
                          bg='#ffcb05', fg='black', font='Helvetica 12 bold')
filename_label.place(relx=0.02, rely=0.27, relwidth=0.15, relheight=0.07)
filename_entry = tk.Entry(frame, bg='white', fg='black',
                          font='Helvetica 12 bold', bd=5)
filename_entry.place(relx=0.20, rely=0.27, relwidth=0.32, relheight=0.07)

testnumber_label = tk.Label(frame, anchor=tk.CENTER, text='Test Number',
                            bg='#ffcb05', fg='black', font='Helvetica 12 bold')
testnumber_label.place(relx=0.02, rely=0.37, relwidth=0.15, relheight=0.07)
testnumber_entry = tk.Entry(frame, bg='white', fg='black',
                            font='Helvetica 12 bold', bd=5)
testnumber_entry.place(relx=0.20, rely=0.37, relwidth=0.32, relheight=0.07)

cellnumber_label = tk.Label(frame, anchor=tk.CENTER, text='Cell Number',
                            bg='#ffcb05', fg='black', font='Helvetica 12 bold')
cellnumber_label.place(relx=0.02, rely=0.47, relwidth=0.15, relheight=0.07)
cellnumber_entry = tk.Entry(frame, bg='white', fg='black',
                            font='Helvetica 12 bold', bd=5)
cellnumber_entry.place(relx=0.20, rely=0.47, relwidth=0.32, relheight=0.07)

instrtype_label = tk.Label(frame, anchor=tk.CENTER, text='Instrument Type',
                           bg='#ffcb05', fg='black', font='Helvetica 12 bold')
instrtype_label.place(relx=0.02, rely=0.57, relwidth=0.15, relheight=0.07)
instrtype_entry = tk.Entry(frame, bg='white', fg='black',
                           font='Helvetica 12 bold', bd=5)
instrtype_entry.place(relx=0.20, rely=0.57, relwidth=0.32, relheight=0.07)

testdescr_label = tk.Label(frame, anchor=tk.CENTER, text='Test Description',
                           bg='#ffcb05', fg='black', font='Helvetica 12 bold')
testdescr_label.place(relx=0.02, rely=0.67, relwidth=0.15, relheight=0.07)
testdescr_entry = tk.Text(frame, bg='white', fg='black',
                          font='Helvetica 9 bold', bd=5)
testdescr_entry.place(relx=0.20, rely=0.67, relwidth=0.32, relheight=0.15)

# find a way to make directory go immediately to data_file_examples (CHECK WHETHER FUNCTION NEEDS TO BE LAMBDA)
upload_button = tk.Button(frame, anchor=tk.CENTER, text='Upload data file...',
                          command=prepopulate, font='Helvetica 12 bold')
upload_button.place(relx=0.375, rely=0.15, relwidth=0.25, relheight=0.08)

# x dropdown (TRY DISABLING FIRST)
x_label = tk.Label(frame, anchor=tk.CENTER, text='X Value', bg='#ffcb05',
                   fg='black', font='Helvetica 12 bold')
x_label.place(relx=0.6, rely=0.27, relwidth=0.1, relheight=0.07)
x_dropdown = tk.OptionMenu(frame, X_SELECT, *X_OPTS)
x_dropdown.place(relx=0.72, rely=0.27, relwidth=0.2, relheight=0.07)

# y dropdown (TRY DISABLING FIRST)
y_label = tk.Label(frame, anchor=tk.CENTER, text='Y Value', bg='#ffcb05',
                   fg='black', font='Helvetica 12 bold')
y_label.place(relx=0.6, rely=0.37, relwidth=0.1, relheight=0.07)
y_dropdown = tk.OptionMenu(frame, Y_SELECT, *Y_OPTS)
y_dropdown.place(relx=0.72, rely=0.37, relwidth=0.2, relheight=0.07)

# data plotting button
data_button = tk.Button(frame, anchor=tk.CENTER, text='Preview Data',
                        command=plot_data, font='Helvetica 12 bold')
data_button.place(relx=0.66, rely=0.5, relwidth=0.2, relheight=0.08)

# link submit button to json_constructor code
submit_button = tk.Button(frame, anchor=tk.CENTER, text='Submit to InfluxDB',
                          command=submit_influxdb, font='Helvetica 12 bold')
submit_button.place(relx=0.375, rely=0.87, relwidth=0.25, relheight=0.08)

root.mainloop()
