#Conexion a BD ideam en  servidor SCADA
import psycopg2
from pandas import DataFrame
import time
import numpy as np
import os
import mysql.connector

### BASE DE DATOS POSTGRES POLARIS-SCADA
def scadaQuery(q):
 
    params = {
      'database': 'ideam',
      'user': os.environ.get('PolarisUser'),
      'password': os.environ.get('polarisPassword') , 
      'host': '172.16.1.193',
      'port': 5432
    }

    try:
        con = psycopg2.connect(**params)
        scada = con.cursor()
        scada.execute(q)
        df2 = DataFrame(scada.fetchall())
        scada.close()
        con.close()
        flag='OK'
    except:

            flag='Error'
            df2=DataFrame()
        
 
    return flag,df2

def scadaQ(q):
    a=['Error',DataFrame()]
    for i in range(0, 2):
        a=scadaQuery(q)
        if a[0] == 'OK':
            break
        else:
            time.sleep(1)
            
    if a[0] == 'Error':
        time.sleep(10)
        for i in range(0, 1):
            a=scadaQuery(q)
            if a[0] == 'OK':
                break
            else:
                time.sleep(2)
    
    return a



### BASE DE DATOS D ELA PALICACION MYSQL LOCAL

def dbQ(q):
    mydb = mysql.connector.connect(
      host="localhost",
      user="root",
      passwd="ideam",
      database="automatizacion"
    )

    mycursor = mydb.cursor()

    mycursor.execute(q)

    mydb.commit()
    try:
        df2 = DataFrame(mycursor.fetchall())
    except:
       df2= DataFrame()
    mycursor.close()
    return df2


def upDateMeasConf():
    q="""SELECT id_stz , id_measure FROM configuration.measures """
    a=scadaQuery(q)[1]
    a.columns= ['id_stz' , 'id_measure']
    for estation in a['id_stz']:
        s=a[a['id_stz']==estation]['id_meadure'].values.to_list()
        s=s.appned(estation)
        q= "UPDATE estaciones \
              SET TAir2m= '%s', TAir10cm='%s', TS10cm='%s', TS30cm='%s', TS50cm='%s', HRAire2m='%s', HRAire10cm='%s',\
              HRSueloCorto='%s', HRSueloMedio='%s', HRSueloLargo='%s', RadG='%s', RadV='%s', Evap='%s', PresAtm='%s',\
              VelVi='%s', DirVi='%s', Prcptcn='%s', Nvl='%s' \ WHERE CODIGO_CAT = %s" 
        q = q % tuple(s)
        print(q)
