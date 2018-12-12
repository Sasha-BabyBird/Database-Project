import mysql.connector
from mysql.connector import Error
from mysql.connector import errorcode
from datetime import datetime, date, time, timedelta
import json
from PIL import Image
'''
I should drop schema every time
'''
def insert_to_table(table, insert_query, records_to_insert, add_once=True):
   global connection
   global cursor   
   cursor.execute(f"select count(*) from {table}")
   count_rows = cursor.fetchone()[0]
   try:
       if table not in insert_query:
            print("Your INSERT SQL query is invalid. Please retry.")
            return
       if add_once:
           if count_rows == 0:
               result  = cursor.executemany(insert_query, records_to_insert)
               print (f"{insert_query} inserted successfully into {table} table")
               connection.commit()
           else:
               print(f'data already inserted in {table}')
        
       else:
            result  = cursor.executemany(insert_query, records_to_insert)
            print (f"{insert_query} inserted successfully into {table} table")
            #транзакции
            connection.commit()
   except mysql.connector.Error as error:
        print("Failed inserting into MySQL table {}".format(error))
        connection.rollback()
   finally:
        return

def insert_testdata():
    global connection
    global cursor
    insert_query = """ INSERT INTO place
                          (room, type) VALUES (%s,%s)"""
   
    records_to_insert = [('livingroom','chair'),
                        ('livingroom','chair'),
                        ('livingroom','wardrobe'),
                        ('smallroom', 'wardrobe'),
                        ('smallroom', 'case')]
    insert_to_table('place', insert_query, records_to_insert)

    insert_query = """ INSERT INTO supertype
                          (name) VALUES (%s)"""
   
    records_to_insert = [('daily',),
                        ('seasonal',),
                        ('special',)]
    insert_to_table('supertype', insert_query, records_to_insert)

    insert_query = """ INSERT INTO category
                       (category_name) VALUES (%s)"""
    records_to_insert = [('tshirt',),
                        ('shirt',),
                        ('coat',),
                        ('trousers',),
                        ('shorts',)]
    insert_to_table('category', insert_query, records_to_insert)


    insert_query = """ INSERT INTO preset
                       (preset_name) VALUES (%s)"""
    records_to_insert = [('heavy',),
                        ('light',),
                        ('summer',),
                        ('occasional',)]
    insert_to_table('preset', insert_query, records_to_insert)


    insert_query = """ INSERT INTO preset_category
                       (preset_preset_id, category_category_id) VALUES (%s,%s)"""
    
    records_to_insert = [
                         (1, 1),
                         (1, 2),
                         (1, 3),
                         (1, 4),
                         (2, 1),
                         (2, 2),
                         (2, 3),
                         (3, 1)
                         ]
    '''
    records_to_insert = [
                       (3, 4),
                       (2, 4)]
    '''
    insert_to_table('preset_category', insert_query, records_to_insert, True)


    insert_query = """ INSERT INTO male_female
                       (value) VALUES (%s)"""
    records_to_insert = [('M',),
                        ('F',),
                        ('U',)]
    insert_to_table('male_female', insert_query, records_to_insert)


    insert_query = """ INSERT INTO clothes_piece
                       (color, size, year_of_purchase, category_id, 
                       male_female_id, stored_at_id) 
                       VALUES (%s,%s,%s,%s,%s,%s)"""
    records_to_insert = [('black', 'M', 2017, 1, 1, 2),
                        ('blue', 'L', 2015, 2, 1, 1),
                        ('yellow', 'M', 2014, 1, 3, 3),
                        ('black', '52', 2016, 3, 3, 1)]
    insert_to_table('clothes_piece', insert_query, records_to_insert)


    insert_query = """ INSERT INTO image
                (image_value, date_modified, clothes_piece_clothes_id)
                 VALUES (%s,%s,%s)"""
    myImage = Image.open('t-shirt.png')
    myImage = myImage.tobytes()
    current_Date = date.today()  
    records_to_insert = [(myImage, current_Date.strftime('%Y-%m-%d'), 1),
                        (myImage, (current_Date+timedelta(days=-1)).strftime('%Y-%m-%d'), 2),
                        (myImage, (current_Date+timedelta(days=-2)).strftime('%Y-%m-%d'), 3),
                        (myImage, (current_Date+timedelta(days=-3)).strftime('%Y-%m-%d'), 4)]
    insert_to_table('image', insert_query, records_to_insert)

    return

if __name__ == '__main__':
    try: 
        connection = mysql.connector.connect(host='localhost',
                                    database='clothes',
                                    user='root',
                                    password='magoga100')
        cursor = connection.cursor(prepared=True)
        
        insert_testdata()

        #closing database connection
        if(connection.is_connected()):
            cursor.close()
            connection.close()
            print("MySQL connection is closed")


    except Error as e :
        print ("Error while connecting to MySQL", e)



 
    

