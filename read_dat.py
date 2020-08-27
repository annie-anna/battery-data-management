import pandas as pd


def read_dat(filepath):
    f = open(filepath, 'r', errors='replace')
    temp = f.readlines()
    f.close()
    i=0
    row=0
    for line in temp:
        i+=1
        line_list = (line.strip('\n')).split()
        if 'ALIAS' in line_list:
            row=i
            print('ALIAS')
            break
    skip_rows=list(range(row-1))
    skip_rows.append([row, row+1])
    df = pd.read_table(filepath, skiprows=skip_rows, encoding='latin', dtype='unicode')
    print(df['qahCell1'][0])
    return df
