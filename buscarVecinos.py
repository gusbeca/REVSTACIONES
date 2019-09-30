#---- LIBRERIAS--- PARA -- CONEXION A BD----------------------------------------------------
import sqlalchemy

import pandas as pd

import geopy.distance


import mysql.connector  #pip install mysql-connector-python
from mysql.connector import errorcode  #Librerias para conexion a BD mysql para guradar resultados
import pymysql


import prestodb
import sys
from pandas import DataFrame


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
    k=0

    for i in range(0,a[0]-1):   #bucle para recorre la lista de estaciones
        if (i * 100 / a[0]) % 1 == 0:
            print(i * 100 / a[0], '%')
        coords_1 =(df.iloc[i]['LATITUD'],df.iloc[i]['LONGITUD'])
        distancia=9999999
        vecino= -1
        for j in range(0,a[0]-1): #Bule para buscar la automatica vecina
            k=k+1
            if j!= i:
                coords_2 = (df.iloc[j]['LATITUD'],df.iloc[j]['LONGITUD'])
                d=geopy.distance.geodesic(coords_1, coords_2).km
                
                if d<distancia:
                    distancia=d
                    vecino= df.iloc[j]['CODIGO_CAT']
                        
            else:
                pass
#----------------------------------------------------------------------------------------
        dfCnvc = lista_convencionales()
        distancia2 = 9999999
        vecinoCnvc=-1
        for q in range(0, dfCnvc.shape[0]-1):# Bucle para buscar la convencional vecina
            coords_2 = (dfCnvc.iloc[q]['lat'], dfCnvc.iloc[q]['long'])
            d = geopy.distance.geodesic(coords_1, coords_2).km
            if d < distancia2:
                distancia2 = d
                if d < 2:
                    vecinoCnvc = int(dfCnvc.iloc[q]['stationid'])

        df.iloc[i,df.columns.get_loc('VECINO')]=vecino
        df.iloc[i,df.columns.get_loc('D_VECINO')]=distancia
        estacion=df.iloc[i,df.columns.get_loc('CODIGO_CAT')]
        q=""" UPDATE automatizacion.estaciones
              SET VECINO = %s, D_VECINO =%s, Convencional_asociada =%s
              WHERE CODIGO_CAT = %s """ % (vecino,distancia,vecinoCnvc,estacion)

        mySQLCursor.execute(q)
#----------------------------------------------------------------------------------------------
    # FUNCION PARA BUSCAR EL VECIMO MAS CERCANO EN LA MISMA CORRIENTE

    for i in range(0, a[0]-1):  # bucle para recorre la lista de estaciones
        vecino = -1
        coords_1 = (df.iloc[i]['LATITUD'], df.iloc[i]['LONGITUD'])
        distancia = 99999

        for j in range(0, a[0]-1):
            if j != i:

                if df.iloc[i]['CORRIENTE'] == df.iloc[j]['CORRIENTE'] and (
                        df.iloc[j]['CLASE'] == 'HID' or df.iloc[j]['CLASE'] == 'HMT'):  # Si estan en la misma corriente

                    coords_2 = (df.iloc[j]['LATITUD'], df.iloc[j]['LONGITUD'])
                    d = geopy.distance.geodesic(coords_1, coords_2).km
                    if d < distancia:
                        distancia = d
                        vecino = df.iloc[j]['CODIGO_CAT']


            else:
                pass
        df.iloc[i, df.columns.get_loc('VECINO_CORREINTE')] = vecino
        df.iloc[i, df.columns.get_loc('D_VECINO_CORRIENTE')] = distancia
        estacion = df.iloc[i, df.columns.get_loc('CODIGO_CAT')]
        
        q = """ UPDATE automatizacion.estaciones
                  SET VECINO_CORREINTE = %s, D_VECINO_CORRIENTE =%s
                  WHERE CODIGO_CAT = %s """ % (vecino, distancia, estacion)

        mySQLCursor.execute(q)
    cnx.commit()
    cnx.close()


def lista_convencionales():
    """Encuentra si la estacion tiene una estacion convencional asociada y la establece en la BD My sql"""
    #1. Establecer conexion con repo en casandra atravez de prestodb
    conn = prestodb.dbapi.connect(
        host='172.16.50.20',
        port=8080,
        user='<REDES>',
        catalog='<cassandra>',
        # schema='<raw>',
    )
    cur = conn.cursor()
    #2 Traer la list ade las estaciones convencionales con su geolocalizacion
    cur.execute("select * from cassandra.raw.stations where brand = 'CONVENTIONAL' LIMIT 10000")
    df = DataFrame(cur.fetchall())
    df.columns = cur.description
    df.columns = [col[0] for col in df.columns]
    return df



veci()
