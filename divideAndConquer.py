#python
import pyodbc #for mssql connection with python
from threading import Thread #for thread
import time
#connect db
conn = pyodbc.connect('Driver={SQL Server};'
                      'Server=USBLRVDEEPAK1;'
                      'Database=dbTemp;'
                      'Trusted_Connection=yes;')
#get table names
def list_of_tables(conn,dbName,howMany):
    try:
        cursor = conn.cursor()
        if howMany == 'ALL':
            statement = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_CATALOG='" + dbName +"'"
        else:
            statement = "SELECT TOP " + howMany + " TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_CATALOG='" + dbName +"'"
        tables = cursor.execute(statement)
        return [table[0] for table in tables]
    except:
        print("Specify input correctly")
        raise

#read the rows
def read(conn,table,folderName):
    cursor = conn.cursor()
    statement = "select * from "+table
    rows = cursor.execute(statement).fetchall()
    filename = folderName+'/'+table+'.txt'
    with open(filename,'w') as file:
        file.write("|".join((str(cur[0]) for cur in cursor.description))+'\n')
        file.write("".join("|".join(map(str,row))+'\n' for row in rows))

#divide and conquer
def merge(conn,tables,folderName,left,right,table_idx,threads):
    if abs(left-right) == 1 and (left not in table_idx) and (right not in table_idx):
        table_idx.add(left)
        table_idx.add(right)
        t = Thread(target = read(conn,tables[left],folderName))
        t.start()
        threads.append(t)
        t = Thread(target = read(conn,tables[right],folderName))
        t.start()
        threads.append(t)
    if left == right and (left not in table_idx):
        table_idx.add(left)
        t = Thread(target = read(conn,tables[left],folderName))
        t.start()
        threads.append(t)
    if left<right:
        mid = (left + right)//2
        merge(conn,tables,folderName,left,mid,table_idx,threads)
        merge(conn,tables,folderName,mid+1,right,table_idx,threads)

#generate for all TABLES
def generateForAll(conn,dbName,folderName):
    print("if you want all tables-> input 'ALL'")
    no_of_tables = input("Enter the number of tables to fetch:")
    tables = list_of_tables(conn,dbName,no_of_tables)
    table_idx = set()
    threads = []
    merge(conn,tables,folderName,0,len(tables)-1,table_idx,threads)
    for thread in threads:
        thread.join()

#generate for one
def generateForParticular(conn,TableName):
    read(conn,TableName,folderName)

generateForAll(conn,'dbTemp','tables')
