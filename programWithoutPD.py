#python
import pyodbc #for mssql connection with python
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
#pipe seperated
def preprocess(values,flag=False):
    st = ''
    for val in values:
        if flag:
            st += str(val[0])+'|'
        else:
            st += str(val)+'|'
    return st[:-1]
#read the rows
def read(conn,table,folderName):
    cursor = conn.cursor()
    statement = "select * from "+table
    rows = cursor.execute(statement)
    filename = folderName+'/'+table+'.txt'
    file = open(filename,'w')
    file.write(preprocess(cursor.description,True)+'\n')
    for row in rows:
        file.write(preprocess(row)+'\n')
    file.close()
#generate for all TABLES
def generateForAll(conn,dbName,folderName):
    tables = list_of_tables(conn,dbName)
    #construct
    for table in tables:
        read(conn,table,folderName)
#generate for one
def generateForParticular(conn,TableName):
    read(conn,TableName,folderName)

#create a folder named tables
generateForAll(conn,'dbTemp','tables')
'''
T = no of tables
R = no of rows of each table
Time complexity : O(TxR)
Space complexity : O(T)
'''
