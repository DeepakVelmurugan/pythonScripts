#python
import pyodbc #for mssql connection with python
import pandas as pd
#connect db
conn = pyodbc.connect('Driver={SQL Server};'
                      'Server=USBLRVDEEPAK1;'
                      'Database=dbTemp;'
                      'Trusted_Connection=yes;')
#get table names
def list_of_tables(conn,dbName):
    cursor = conn.cursor()
    statement = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_CATALOG='" + dbName +"'"
    tables = cursor.execute(statement)
    return [table[0] for table in tables]
#read the rows
def read(conn,table,folderPath):
    cursor = conn.cursor()
    statement = "select * from "+table
    sql_query = pd.read_sql_query(statement,conn)
    df = pd.DataFrame(sql_query)
    path = "C:\\Users\\vdeepak\\Desktop\\"+table+'.csv'
    df.to_csv(path,index=False,sep='|')
#generate for all TABLES
def generateForAll(conn,dbName,folderPath):
    tables = list_of_tables(conn,dbName)
    #construct
    for table in tables:
        read(conn,table,folderPath)
#generate for one
def generateForParticular(conn,TableName,folderPath):
    read(conn,TableName,folderPath)

#change the path name
path = "C:\\Users\\vdeepak\\Desktop\\"
#generateForAll(conn,'dbTemp',path)
generateForParticular(conn,'info',path)
'''
T = no of tables
R = no of rows of each table
Time complexity : O(TxR)
Space complexity : O(T)
'''
