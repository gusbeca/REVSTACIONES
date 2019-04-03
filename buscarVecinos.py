#---- LIBRERIAS--- PARA -- CONEXION A BD----------------------------------------------------
import sqlalchemy

import pandas as pd

import geopy.distance


import mysql.connector  #pip install mysql-connector-python
from mysql.connector import errorcode  #Librerias para conexion a BD mysql para guradar resultados
import pymysql

def veci():
    q="""select * from estaciones """
    engine=sqlalchemy.create_engine('mysql+pymysql://root:ideam@localhost:3306/automatizacion')
    df=pd.read_sql_query(q, engine)

    try:
      config = {
                  'user': 'root',
                  'password': 'ideam',
                  'host': '127.0.0.1',
                  'database': 'automatizacion',
                  'raise_on_warnings': True,
                 }

      cnx = mysql.connector.connect(**config)
      mySQLCursor = cnx.cursor()
    except mysql.connector.Error as err:
      if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
        print("Something is wrong with your user name or password")
      elif err.errno == errorcode.ER_BAD_DB_ERROR:
        print("Database does not exist")
      else:
        print(err)


    #FUNCION PARA BUSCAR EL VECIMO MAS CERCANO

    a=df.shape
    for i in range(0,a[0]):   #bucle para recorre la lista de estaciones
        coords_1 =(df.iloc[i]['LATITUD'],df.iloc[i]['LONGITUD'])
        distancia=9999999
        vecino= -1
        for j in range(0,a[0]):
            if j!= i:
                coords_2 = (df.iloc[j]['LATITUD'],df.iloc[j]['LONGITUD'])
                d=geopy.distance.vincenty(coords_1, coords_2).km
                if d<distancia:
                    distancia=d
                    vecino= df.iloc[j]['CODIGO_CAT']
            else:
                pass
        df.iloc[i,df.columns.get_loc('VECINO')]=vecino
        df.iloc[i,df.columns.get_loc('D_VECINO')]=distancia
        estacion=df.iloc[i,df.columns.get_loc('CODIGO_CAT')]
        q=""" UPDATE estaciones
              SET VECINO = %s, D_VECINO =%s
              WHERE CODIGO_CAT > %s """ % (vecino,distancia,estacion)

        mySQLCursor.execute(q)

    # FUNCION PARA BUSCAR EL VECIMO MAS CERCANO EN LA MISMA CORRIENTE

    for i in range(0, a[0]):  # bucle para recorre la lista de estaciones
        vecino = -1
        coords_1 = (df.iloc[i]['LATITUD'], df.iloc[i]['LONGITUD'])
        distancia = 99999

        for j in range(0, a[0]):
            if j != i:

                if df.iloc[i]['CORRIENTE'] == df.iloc[j]['CORRIENTE'] and (
                        df.iloc[j]['CLASE'] == 'HID' or df.iloc[j]['CLASE'] == 'HMT'):  # Si estan en la misma corriente

                    coords_2 = (df.iloc[j]['LATITUD'], df.iloc[j]['LONGITUD'])
                    d = geopy.distance.vincenty(coords_1, coords_2).km
                    if d < distancia:
                        distancia = d
                        vecino = df.iloc[j]['CODIGO_CAT']
                        # print(vecino)

            else:
                pass
        df.iloc[i, df.columns.get_loc('VECINO_CORREINTE')] = vecino
        df.iloc[i, df.columns.get_loc('D_VECINO_CORRIENTE')] = distancia
        estacion = df.iloc[i, df.columns.get_loc('CODIGO_CAT')]
        q = """ UPDATE estaciones
                  SET VECINO_CORREINTE = %s, D_VECINO_CORRIENTE =%s
                  WHERE CODIGO_CAT > %s """ % (vecino, distancia, estacion)

        mySQLCursor.execute(q)