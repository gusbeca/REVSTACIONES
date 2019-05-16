#Consulta BD hydas y polaris y usa las funciones de la libreria rojillas para determinar el estado de la red, finalmente almacena en BD mysql
import os
import rojillas
import bibi
import tecnico
import time
from pandas import DataFrame
import pandas as pd
import sqlalchemy
import numpy as np
from datetime import datetime, timedelta


import mysql.connector
from mysql.connector import errorcode  #Librerias para conexion a BD mysql para guradar resultados
import pymysql
import sqlalchemy




# Cargar maestro de estaciones de BD mysql local-------------------------------------------------------------------------------------------------

q = """select * from estaciones """   # WHERE estaciones.ESTADO2 ='0' LEFT JOIN revision ON estaciones.CODIGO_CAT = revision.CODIGO_CAT
engine = sqlalchemy.create_engine('mysql+pymysql://root:ideam@localhost:3306/automatizacion')
df = pd.read_sql_query(q, engine)
#print(df.head())
campos = df.columns.values.tolist()


q = """select * from revision    
     limit 1"""   #LEFT JOIN revision ON estaciones.CODIGO_CAT = revision.CODIGO_CAT
engine = sqlalchemy.create_engine('mysql+pymysql://root:ideam@localhost:3306/automatizacion')
dfb = pd.read_sql_query(q, engine)
camposb = dfb.columns.values.tolist()

camposb.remove('id')
for i in campos:
    if i in camposb:
        camposb.remove(i)


for i in camposb:
    df[i]=np.nan



start = time.time()

df = df[df.TIPO != 'CON']
df = df[df.TIPO != 'AUS']
#df = df[df.SINIESTRO == 0]

a = df.shape
inicio=0
fin= a[0]
df=df.iloc[inicio:fin] #Para particionar el dataframe
a = df.shape

retorno=tecnico.revisarRed(inicio, fin, df)
if retorno ==1:
    time.sleep(1800)
    for i in range(0, 4):
        retorno=tecnico.revisarRed(inicio, fin, df)
        if retorno == 0:
            break
        else:
            time.sleep(1200)

#sched.start()


