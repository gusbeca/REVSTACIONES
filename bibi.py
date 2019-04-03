#Conexion a BD ideam en  servidor SCADA
import psycopg2
from pandas import DataFrame
import time
import numpy as np
import os


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


