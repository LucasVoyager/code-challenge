import os 
import pandas as pd # type: ignore
from datetime import datetime
from sqlalchemy import create_engine # type: ignore

def criarRepositorios(basePath,source,tableName):
    dateStr =  datetime.now().strftime('%d%m%Y')
    path = os.path.join(basePath,source,tableName,dateStr)
    os.makedirs(path,existOk=True)
    return path

def extrairCsv(pathCsv):
    data = pd.read_csv(pathCsv)
    return data

def extrairPostgres(conStr,tableName):
    engine = create_engine(conStr)
    query = f"SELECT * FROM {tableName}"
    data = pd.read_sql(query, engine)
    return data

def salvarDados(data,path,filename):
    filePath = os.path.join(path,filename)
    data.to_csv(filePath, index=False)

def carregarPostgres(connecStr,path,tableName):
    engine = create_engine(connecStr)
    for file in os.listdir(path):
        if file.endswith('.csv'):
            filePath = os.path.join(path,file)
            data = pd.read_csv(filePath)
            data.to_sql(tableName, engine, if_exist="append", index=False)


basePath = '/data'

pathCSV = 'order_details.csv'
postgresConStr = 'postgresql://northwind_user:thewindisblowing@localhost:5432/northwind'
tableName = 'order_details'

dataCSV = extrairCsv(pathCSV)
csvSaveData = criarRepositorios(basePath,'csv', tableName)
salvarDados(dataCSV, pathCSV, 'data.csv')

dataPostgres = extrairPostgres(postgresConStr, tableName)
savePostgresData = criarRepositorios(basePath, 'postgres', tableName)
salvarDados(dataPostgres, savePostgresData, 'data.csv')

carregarPostgres(postgresConStr, csvSaveData, tableName)
carregarPostgres(postgresConStr, savePostgresData, tableName)