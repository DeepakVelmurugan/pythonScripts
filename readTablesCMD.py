import os
import time
import sys
import pyodbc #for mssql connection with python

#connect db
def connect(dbName):
    try:
        conn = pyodbc.connect('Driver={SQL Server};'
                              'Server=USBLRVDEEPAK1;'
                              'Database='+dbName+';'
                              'Trusted_Connection=yes;')
    except:
        print("Connection error! Check dbName")
        sys.exit()
    return conn

#get table names
def list_of_tables(conn,dbName):
    try:
        cursor = conn.cursor()
        statement = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_CATALOG='" + dbName +"'"
        tables = cursor.execute(statement)
        return [table[0] for table in tables]
    except:
        print("Error! Check your db name")
        sys.exit()

def generateForAll():
    dbName = input("Enter the database name:")
    conn = connect(dbName)
    print("if you want all tables-> input 'ALL'")
    tables = list(map(str,input("Enter the names of tables to fetch: ").split()))
    schema = input("Enter for which schema: ")
    if(tables[0] == "ALL"):
        tables = list_of_tables(conn,dbName)
    if(len(tables)==0):
        print("Error! Check dbName or DB")
        sys.exit()
    s = time.time()
    for table in tables:
        statement = 'sqlcmd -s"|" -W -Q "set nocount on; select * from [' + dbName + '].['+schema+'].['+table+']" | findstr /v /c:"-" /b > "'+table+'.csv"'
        os.system(statement)
    e = time.time()
    print("Time taken(secs): ",e-s)

generateForAll()
