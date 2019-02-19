import mysql.connector
from mysql.connector import Error
from mysql.connector import errorcode
from datetime import datetime, date, time, timedelta
import prettytable
import json
from PIL import Image


def table_to_json(table, filename=None):
    global connection
    global cursor
    tables = []
    cursor.execute('show tables;')
    for tup in cursor.fetchall():
        tables.append(tup[0])
    if table not in tables:
        print('Requested table does not exist!')
        return
    if filename is None:
        filename = f'{table}.json'
    cursor.execute(f'SELECT * FROM {table}')
    row_headers = [x[0] for x in cursor.description]
    json_data = [dict(zip(row_headers, row)) for row in cursor.fetchall()]
    with open(filename, 'w+') as jsonfile:
        json.dump(json_data, jsonfile, ensure_ascii=False,
                  indent=4, separators=(', ', ' : '))
    print('done')
    return


def db_to_json(filename=None):
    global connection
    global cursor
    if filename is None:
        filename = f'clothes_db.json'
    tables = (['category', 'preset_category', 'preset',
               'male_female', 'place', 'clothes_piece', 'supertype'])
    big_json_data = {}
    for table in tables:
        cursor.execute(f'SELECT * FROM {table}')
        row_headers = [x[0] for x in cursor.description]
        json_data = ([dict(zip(row_headers, row))
                      for row in cursor.fetchall()])
        for key in json_data:
            #print(key)
            for valkey in key:
                if isinstance(key[valkey], date):
                    key[valkey] = str(key[valkey])
        big_json_data[table] = json_data
    with open(filename, 'w+') as jsonfile:
        json.dump(big_json_data, jsonfile, ensure_ascii=False,
                  indent=4, separators=(', ', ' : '))
    print('done')
    return


def create_query_and_select(table, show_query=False, **kwargs):
    global connection
    global cursor
    tables = []
    cursor.execute('show tables;')
    for tup in cursor.fetchall():
        tables.append(tup[0])
    if table not in tables:
        print("Requested table does not exist!")
        return

    select_query = f'SELECT * FROM {table} '

    is_first = True
    brackets_counter = 0
    for key in kwargs:
        if is_first:

            if isinstance(kwargs[key], list) and len(kwargs[key]) > 1:
                select_query += f'where ({key}='
                for i in range(len(kwargs[key]) - 1):
                    select_query += f'{kwargs[key][i]!r} or ({key}='
                    brackets_counter += 1
                select_query += f'{kwargs[key][-1]!r} '
                for i in range(brackets_counter):
                    select_query += ')'
                brackets_counter = 0
                select_query += ') '
            elif isinstance(kwargs[key], list) and len(kwargs[key]) == 1:
                select_query += f'where {key}={kwargs[key][0]!r} '
            elif isinstance(kwargs[key], list):
                continue
            else:
                select_query += f'where {key}={kwargs[key]!r} '
            is_first = False

        else:
            brackets_counter += 1
            stored_bc = brackets_counter
            if isinstance(kwargs[key], list) and len(kwargs[key]) > 1:
                select_query += f'and (({key}='
                brackets_counter = 0
                for i in range(len(kwargs[key]) - 1):
                    select_query += f'and ({kwargs[key][i]!r} or ({key}='
                    brackets_counter += 1
                select_query += f'and ({kwargs[key][-1]!r} '
                for i in range(brackets_counter):
                    select_query += ')'
                brackets_counter = stored_bc
                select_query += ')'
            elif isinstance(kwargs[key], list) and len(kwargs[key]) == 1:
                select_query += f'and ({key}={kwargs[key][0]!r} '
            elif isinstance(kwargs[key], list):
                continue
            else:
                select_query += f'and ({key}={kwargs[key]!r} '
            for i in range(brackets_counter):
                select_query += ') '

    if show_query:
        print(select_query)
    cursor.execute(select_query)
    output = prettytable.from_db_cursor(cursor)
    print(output)
    return


def select_specific_clothes(value, show_query = True):
    global connection
    global cursor
    select_query = f'''select * from clothes_piece
                      where clothes_id in
                    (select (clothes_piece.clothes_id)
                     from clothes_piece join category
                     where clothes_piece.category_id = 
                     (select category.category_id
                    where category.category_name = 
                    {value!r}))'''
    if show_query:
        print(select_query)
    cursor.execute(select_query)
    output = prettytable.from_db_cursor(cursor)
    print(output)
    return


def insert_to_table(table, insert_query, to_insert):
    global connection
    global cursor
    try:
        if table not in insert_query:
            print("Your INSERT SQL query is invalid. Please retry.")
            return

        result = cursor.execute(insert_query, to_insert)
        print(f"Inserted successfully into {table} table")
        cursor.execute(f'SELECT * FROM {table}')
        output = prettytable.from_db_cursor(cursor)
        print(output)
        # транзакции
        connection.commit()
    except mysql.connector.Error as error:
        print("Failed inserting into MySQL table {}".format(error))
        connection.rollback()
    finally:
        return


def create_query_and_insert(table, show_query=False, **kwargs):
    global connection
    global cursor
    tables = []
    cursor.execute('show tables;')
    for tup in cursor.fetchall():
        tables.append(tup[0])
    if table not in tables:
        print("Requested table does not exist!")
        return

    insert_query = f'INSERT INTO {table} ('
    is_first = True
    pquery = ''
    to_insert = []
    for key in kwargs:
        if is_first:
            insert_query += f'{key}'
            pquery += '%s'
            is_first = False
        else:
            insert_query += f', {key}'
            pquery += ',%s'
        to_insert.append(kwargs[key])
    insert_query += f') VALUES ({pquery})'
    to_insert = tuple(to_insert)
    if show_query:
        print(insert_query)
        print(to_insert)
    insert_to_table(table, insert_query, to_insert)
    return


def get_columns_info(table):
    global connection
    global cursor
    tables = []
    cursor.execute('show tables;')
    for tup in cursor.fetchall():
        tables.append(tup[0])
    if table not in tables:
        print("Requested table does not exist!")
        return
    columns_info = []
    cursor.execute(f'describe {table};')
    columns = cursor.fetchall()
    for col in columns:
        #print(col)
        if col[5] != 'auto_increment':
            if col[2] == 'YES' or col[4] is not None:
                if col[4] is not None:
                    columns_info.append(
                        f'{col[0]} (optional) (default: {col[4]})')
                else:
                    columns_info.append(f'{col[0]} (optional)')
            else:
                columns_info.append(f'{col[0]}')

    return columns_info


def get_columns_dict(table):
    global connection
    global cursor
    tables = []
    cursor.execute('show tables;')
    for tup in cursor.fetchall():
        tables.append(tup[0])
    if table not in tables:
        print("Requested table does not exist!")
        return
    columns_dict = {}
    cursor.execute(f'describe {table};')
    columns = cursor.fetchall()
    for col in columns:
        if col[5] != 'auto_increment':
            columns_dict[col[0]] = []

    return columns_dict


def select_table_from_db(flag='select'):
    global connection
    global cursor
    tables = []
    cursor.execute('show tables;')
    for tup in cursor.fetchall():
        tables.append(tup[0])
    print(f'\nSelect a table to {flag}:')
    for i in range(len(tables)):
        print(f'{i+1}. {tables[i]}')
    print(f'\n0. Back')
    while True:
        table_num = input('Type a digit to open the corresponding table:\n')
        if table_num.isdecimal():

            if int(table_num) in range(1, len(tables)+1):
                operate_table(tables[int(table_num)-1], flag)
                break
            elif int(table_num) == 0:
                main_menu()
                break
            else:
                print('Invalid input. Please retry.')
                continue
        else:
            print('Invalid input. Please retry.')
            continue
    return


def operate_table(table, flag='select'):
    global connection
    global cursor
    tables = []
    cursor.execute('show tables;')
    for tup in cursor.fetchall():
        tables.append(tup[0])
    if table not in tables:
        print("Requested table does not exist!")
        return
    columns = get_columns_info(table)
    kwargs = get_columns_dict(table)

    while True:
        print('----------------')
        print(f'Table: {table}')
        if flag == 'select':
            print('Select a condition:')
            print(f'S. Show table with inserted conditions')
            print(f'C. Clear all conditions')
            print(f'Current conditions set: {kwargs}')
            for i in range(len(columns)):
                print(f'{i+1}. {columns[i]}')
            print(f'\n0. Back')
            attr_num = input(
                'Type a digit to add the corresponding condition:\n')
            if attr_num.isdecimal():
                if int(attr_num) in range(1, len(columns)+1):
                    while True:
                        print(
                            f'1. Add possible condition to {list(kwargs)[int(attr_num) - 1]}')
                        print(f'\n0. Back')
                        val = input('Type...\n')
                        if val.isdecimal():
                            if int(val) == 1:
                                print(kwargs[list(kwargs)[int(attr_num) - 1]])
                                cond = input('Type a new condition value...\n')
                                if cond != '':
                                    kwargs[list(kwargs)[
                                        int(attr_num) - 1]].append(cond)
                                    continue
                                else:
                                    print('\nYou skipped an input! Please retry.')
                                    break
                            if int(val) == 0:
                                break
                            else:
                                print('Invalid input. Please retry.')
                                continue
                        else:
                            print('Invalid input. Please retry.')
                            continue

                elif int(attr_num) == 0:
                    select_table_from_db(flag='select')
                    break
                else:
                    print('Invalid input. Please retry.')
                    continue
            elif attr_num == 'S' or attr_num == 's':
                print(kwargs)
                create_query_and_select(table, True, **kwargs)
                input('press any key to continue...\n')

            elif attr_num == 'C' or attr_num == 'c':
                for key in kwargs:
                    kwargs[key] = []
                print('\nConditons cleared')
            else:
                print('Invalid input. Please retry.')
                continue

        elif flag == 'insert':
            cursor.execute(f"select count(*) from {table};")
            count_rows = cursor.fetchone()[0]
            for key in kwargs:
                kwargs[key] = ''
            print(f'{table} currently has {count_rows} records')
            print(f'1. Insert data into {table}')
            print(f'\n0. Back')
            choice = input('Type...\n')
            cancelled = False
            newkwargs = {}
            if choice.isdecimal():
                if int(choice) == 1:
                    for i in range(len(columns)):
                        print(f'{i+1}. {columns[i]}')
                        if '(optional)' in columns[i]:
                            val = (input('''Insert value of the column. Insert '*' to pass it. ''' +
                                         '''\nInsert 'cancel' to cancel operation\n'''))
                            if val == '*':
                                continue
                            elif val == 'cancel' or val == '':
                                cancelled = True
                                break
                            elif val.isdecimal():
                                if int(val) > 255 or int(val) < 0:
                                    print('Warning! This may be out of bounds! Use at your own risk!')
                                    newkwargs[list(kwargs)[i]] = val
                                else:
                                    newkwargs[list(kwargs)[i]] = val
                            else:
                                newkwargs[list(kwargs)[i]] = val
                        else:
                            val = (input('''Insert value of the column. This is mandatory, otherwise error. ''' +
                                         '''\nInsert 'cancel' to cancel operation\n'''))
                            if val in ['cancel', '', '*']:
                                cancelled = True
                                break
                            if val.isdecimal():
                                if int(val) > 255 or int(val) < 0:
                                    print('Warning! This may be out of bounds! Use at your own risk!')
                                    newkwargs[list(kwargs)[i]] = val
                                else:
                                    newkwargs[list(kwargs)[i]] = val
                            else:
                                newkwargs[list(kwargs)[i]] = val
                    print('\n')
                    print(newkwargs)
                    if not cancelled:
                        print('1. Confirm insert operation')
                        print('\n0. Cancel and go back\n')
                        confirm = input('Confirm or cancel?\n')
                        if confirm.isdecimal():
                            if int(confirm) == 1:
                                create_query_and_insert(
                                    table, True, **newkwargs)
                                break

                            elif int(confirm) == 0:
                                print('\nCancelled successfully\n')
                                continue
                            else:
                                print('Invalid input. Please retry.')
                                continue
                        else:
                            print('Invalid input. Please retry.')
                            continue
                    else:
                        print('\nCancelled successfully\n')
                        continue

                elif int(choice) == 0:
                    select_table_from_db(flag='insert')
                    break
                else:
                    print('Invalid input. Please retry.')
                    continue
            else:
                print('Invalid input. Please retry.')
                continue

            break
    return


def main_menu():
    global connection
    global cursor
    #tables = (['category', 'preset_category', 'preset',
               #'male_female', 'place', 'clothes_piece', 'supertype'])
    tables = []
    cursor.execute('show tables;')
    for tup in cursor.fetchall():
        tables.append(tup[0])
    while True:
        print('-----------------')
        print('1. Show all tables')
        print('2. Convert a table into json')
        print('3. Convert DB into json')
        print('4. Select info from a table')
        print('5. Insert new data into a table')
        print('6. Select all pieces of clothes with a specific category')
        print('\n0. Quit')
        choice = input('Choose an option:\n')
        if choice.isdecimal():
            if int(choice) == 1:
                for table in tables:
                    print(table)
                continue
            elif int(choice) == 2:
                table_name = input('Type a name of a desired table:\n')
                table_to_json(table_name)
                continue
            elif int(choice) == 3:
                db_to_json()
                continue
            elif int(choice) == 4:
                select_table_from_db()
                break
            elif int(choice) == 5:
                select_table_from_db('insert')
                continue
            elif int(choice) == 6:
                value = input('Insert category value:\n')
                select_specific_clothes(value)
                continue
            elif int(choice) == 0:
                break
            else:
                print('Invalid input. Please retry')
                continue
        else:
            print('Invalid input. Please retry')
            continue
    return


if __name__ == '__main__':
    try:
        connection = mysql.connector.connect(host='localhost',
                                             database='clothes',
                                             user='root',
                                             password='magoga100',
                                             use_pure=True)
        cursor = connection.cursor(prepared=True)
        main_menu()
        #select_specific_clothes('tshirt')
        #print(get_columns_info('clothes_piece'))
        #create_query_and_insert('place', True, type='very_big_chair')
        #db_to_json()
        #create_query_and_select('clothes_piece', show_query=True, color=['black', 'yellow'], size=[])
		
        #closing database connection
        if(connection.is_connected()):
            cursor.close()
            connection.close()
            print("MySQL connection is closed")

    except Error as e:
        print("Error while connecting to MySQL", e)
