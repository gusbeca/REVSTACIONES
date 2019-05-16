from buscarVecinos import veci
#-----------------------Librerias par conexion a BDs--------------------------------------------------------------------
import psycopg2 #Para conexion a PostGresSQL BD SCADA
import prestodb #Cliente Presto DB para python para conexion a Repositorio Hydras3 Cassandra pip install presto-python-client
import mysql.connector
from mysql.connector import errorcode  #Librerias para conexion a BD mysql para guradar resultados
import pymysql
import sqlalchemy
#-----------------------------------------------------------------------------------------------------------------------
#Librerias de estadistica y matematicas
import statsmodels.api as sm #require patsy
import numpy as np
import numpy
from sklearn import datasets, linear_model
from scipy import stats
from pandas import DataFrame
import pandas as pd
#-----------------------------------------------------------------------------------------------------------------------
#Crawler para traer datos DCP Status
import urllib3
from bs4 import BeautifulSoup
#-----------------------------------------------------------------------------------------------------------------------
#Otras librerias
import pprint
import sys
from datetime import datetime, timedelta
import time
import bibi
import os
#-----------------------------------------------------------------------------------------------------------------------
#Conexion a repositorio Cassandra a traves de prestoDB (Datos Hydras3)
conn=prestodb.dbapi.connect(
    host='172.16.50.20',
    port=8080,
    user=os.environ.get('HydrasUser'),
    catalog='<cassandra>',
    schema='<raw>',
)
hydras = conn.cursor()
#-----------------------------------------------------------------------------------------------------------------------
#--CONSTANTES
ventanaObs= 7 # Ventana de tiempo donde se evalua o revisa la estacion o sensor
#-------------------------------------------------
def hydrasq(estacion):
    try:

        q = """select * 
                         from cassandra.raw.last_observations 
                         where station = '%s' and 
                         event_value IS NOT NULL AND  regexp_like(sensor, '\d+')
                         ORDER BY event_time DESC
                         """ % estacion

        hydras.execute(q)
        df2 = DataFrame(hydras.fetchall())
        q= """SHOW COLUMNS FROM cassandra.raw.last_observations  """
        hydras.execute(q)
        dfhead= DataFrame(hydras.fetchall())
       
            
        df2.columns = list(dfhead.iloc[:, 0])
        df2=df2[["station", "sensor", "event_time", "event_value"]]
        df2['event_time'] = pd.to_datetime(df2['event_time'], infer_datetime_format=True)
        df2.event_value.astype('float64')
            # df2.drop_duplicates(subset='sensor', keep='first', inplace=False)
        df2.set_index('event_time')
    except:
        df2 = DataFrame()
        
    return df2
#----TEST DE LA FUNCION-----------------------


#BORRAR=hydrasq('0035027002')
#print(BORRAR.shape)
#print(max(BORRAR['event_time']))

#print(BORRAR.head())
#print(BORRAR.sensor.unique())


#--------------------------------------------------------------------------------------------------------------------------
def polarisq(ID_STZ_SCADA):
    
    df2 = DataFrame()
    q = """select id_stz, id_measure, date_record, val_data from data_radio.archive_data 
          where id_stz = %s and val_data IS NOT NULL
          ORDER BY date_record DESC limit 30""" % ID_STZ_SCADA  # Se limita a 30 pra reducir tiempos

    consulta= bibi.scadaQuery(q)
    df2 = consulta[1]
    qs = df2.shape
    if qs[0] > 0:
        df2.columns = ["station", "sensor", "event_time", "event_value"]
        df2['event_time'] = pd.to_datetime(df2['event_time'], infer_datetime_format=True)
        df2.event_value.astype('float64')
        df2.set_index('event_time')
    else:
        df2 = DataFrame()

    return df2
#---TEST DE LA FUNCION------------------------------------------------------
#df2=polarisq(228)
#df2.shape
#print(df2.head())
#print(df2.sensor.unique())
#-----------------------------------------------------------------------------------------------------------------------
#---REVISA EL ESTADO ON-LINE/OFF-LINE DE LA ESTACION PRIMERO EN SCADDA Y SI NO HEN HYDRAS Y SI NO PAILA
def StateQuery(**kwargs):
    estacion = kwargs['codigo_catalogo']
    ID_STZ_SCADA = kwargs['IDSCADA']
   
    pase = 0
    x = [-1, -1, [], 'DESCONOCIDO']

    dfp = polarisq(ID_STZ_SCADA)

    qp = dfp.shape

    if qp[0] > 0:
        LAST_DATEp = max(dfp['event_time'])
        
        if LAST_DATEp >= datetime.now() - timedelta(days=1.1): # Si el ultimo dato en scada tiene menos de 1.1 dias de antiguedad pasa sino mira hydras
            x[0] = 'SCADA'
            df2 = dfp
        else:
            pase = 1
    else:
        pase = 2
        LAST_DATEh = datetime.strptime('1900-01-01 00:00:00', "%Y-%m-%d %H:%M:%S")

    if pase != 0:
        dfh = hydrasq(estacion)
        qh = dfh.shape

        if qh[0] > 0 and pase == 1:
            LAST_DATEh = max(dfh['event_time'])
            if LAST_DATEp >= LAST_DATEh:
                x[0] = 'SCADA'
                df2 = dfp
            else:
                x[0] = 'HYDRAS'
                df2 = dfh
        elif qh[0] > 0 and pase == 2:
            x[0] = 'HYDRAS'
            df2 = dfh
        elif qp[0]>qh[0] :
            x[0] = 'SCADA'
            df2 = dfp
    
    try:
        if x[0] == 'SCADA':
            x[1] = max(df2['event_time'])
            q = """select distinct id_measure
                  from configuration.measures
                  where id_stz = %s """ % ID_STZ_SCADA  
            consulta=bibi.scadaQuery(q)
            dfS = consulta[1]
            qs = dfS.shape

            if qs[0] > 0:
                dfS.columns = ["sensor"]
            else:
                dfS = DataFrame()

            x[2] = dfS.sensor.unique()
        else:
            x[1] = max(df2['event_time']) + timedelta(hours=5)
            x[2] = df2.sensor.unique()
    except:
        x[1] = datetime.strptime('1900-01-01 00:00:00', "%Y-%m-%d %H:%M:%S")
        x[2] = []

    if x[1] >= datetime.now() - timedelta(1):  # Nuero de dias para considerar una estacion off liene
        x[3] = 'ON_LINE'
    elif x[0]==-1 and ID_STZ_SCADA != -1:
        x[3] = 'DESCONOCIDO'
    else:
        x[3] = 'OFF_LINE'
    return x

#-----TEST DE LA FUNCION-------------------
#s={'codigo_catalogo':'0011115020', 'IDSCADA':35}
#print((StateQuery(**s)))
#-----------------------------------------------------------------------------------------------------------------------
# Funcion para consultar estado DCP en http://www.sutronwin.com/dcpmon/------------------------------------------

def dcpmon(**kwargs):

    dcpaddress = kwargs['DCP_Address']
    if dcpaddress != '-1' or dcpaddress != '':
        http = urllib3.PoolManager()  # Instanciamiento de objeto urlib para consulta web
        url = "http://www.sutronwin.com/dcpmon/dcpout.jsp"  # Direccio web para consultas
        try:
            r = http.request('GET', url,
                             fields={'group_select': 'Southern-California-Edison', 'channel_select': 'Channel_1',
                                     'select_option': 'dcp_text', 'dcp_text': dcpaddress, 'date_range': '5',
                                     'get_report': 'Get+Report'})  # fields={'dcp_text': 'CB0121EE'}

            soup = BeautifulSoup(r.data, 'html.parser')  # Parser de pagina web para extraer datos
            qn = len(soup.find_all('tr', class_="full_perf_report"))

            if qn == 0:
                return ['DESCONOCIDO', -1]  # Si la tabla de estatus no contiene filas retorna error

            else:
                est = soup.find_all('tr', class_="full_perf_report")[0].find_all('td')[6].text  # Estado de tX G=Good
                senal = int(soup.find_all('tr', class_="full_perf_report")[0].find_all('td')[7].text)  # Nivel de Señal dB

                # Verificar Intermitencia en transmision

                lista = soup.find_all('tr', class_="full_perf_report")
                i = 0
                c = []
                e = []
                intermitencia = 0
                for tr in lista:
                    a = str(tr.find_all('td')[6].text).replace(' ', '')[0]
                    if i > 0:
                        if b == a:
                            pass
                        else:
                            intermitencia = intermitencia + 1

                    b = a
                    i = i + 1
                    x = int(tr.find_all('td')[7].text)
                    if x > 0:
                        c.append(x)
                    e.append(x)
                    # -----------------------------------
                    # Evaluando si la intensidad de la señal esta decreciendo

                x1 = pd.DataFrame({'x': list(range(0, len(c)))})
                y1 = pd.DataFrame({'y': c})
                x1 = x1.values
                y1 = y1.values
                s = x1.shape
                length = s[0]
                x = x1.reshape(length, 1)
                y = y1.reshape(length, 1)
                regr = linear_model.LinearRegression()
                regr.fit(x1, y1)
                b = regr.intercept_
                m = regr.coef_
                # print(b,"----",m)
                e = max(e[:18])
                # ------------------------------------
                if intermitencia > 2 and e > 28:
                    return ["INTERMITENTE", senal]
                elif m < -0.017 and e > 28:
                    return ["DETERIORANDOSE", senal]
                elif est == "G" and senal < 33:
                    return ["BAJA_SEÑAL", senal]
                elif est == "G" and senal >= 33:
                    return ["OK", senal]
                else:
                    return ["FUSER", senal]

        except:
            return ['DESCONOCIDO', np.nan]  # Si no se encuentra el DCP retorna error

#--Prueba de la funcion----------------------------
#para={'DCP_Address':'CB0121EE','a':'CB0121xx'}
#print(dcpmon(**para))
#-----------------------------------------------------------------------------------------------------------------------
# cONSULTA DE SENSORES EN HYDRAS SINO ENCUENTRA EN SCADA BSUCA EN HYDRAS

def SensorQuery(estacion, ID_STZ_SCADA, sensorH, sensorS, days_to_subtract, servidor):
    if days_to_subtract != np.nan and days_to_subtract!= pd.NaT:
        FECHA_HORA_INI = str(datetime.now() - timedelta(5) - timedelta(days=days_to_subtract))
        
        #print(estacion, ' :', 'FECHA_HORA_INI: ', FECHA_HORA_INI, sensorH)
        df2 = DataFrame()
        if servidor == 'HYDRAS':

            q = """select * 
                         from cassandra.raw.last_month_observations 
                         where station = '%s' and sensor = '%s' and
                         event_value IS NOT NULL and
                         format_datetime(event_time, 'yyyy-MM-dd HH:mm:ss') 
                         BETWEEN '%s' AND '2050-11-07 01:00:00'
                         ORDER BY event_time DESC
                         """ % (estacion, sensorH, FECHA_HORA_INI)

            try:
                hydras.execute(q)
                df2 = DataFrame(hydras.fetchall())
                qh = df2.shape
                #print(estacion, ' :', 'FECHA_HORA_INI: ', FECHA_HORA_INI, sensorH, ' registros: ', df2.shape[0])
                if qh[0] < 1:
                    q = """select * 
                                             from cassandra.raw.last_observations 
                                             where station = '%s' and sensor ='%s' and
                                             event_value IS NOT NULL and 
                                             format_datetime(event_time, 'yyyy-MM-dd HH:mm:ss') 
                                             BETWEEN '%s' AND '2050-11-07 01:00:00'
                                             ORDER BY event_time DESC
                           """ % (estacion, sensorH, FECHA_HORA_INI)
                    hydras.execute(q)
                    df2 = DataFrame(hydras.fetchall())
                    qh = df2.shape

                if qh[0] > 0:
                    
                    q= """SHOW COLUMNS FROM cassandra.raw.last_month_observations  """
                    hydras.execute(q)
                    dfhead= DataFrame(hydras.fetchall())
                    df2.columns = list(dfhead.iloc[:, 0])
                    df2 = df2[["station", "sensor", "event_time", "event_value"]]
                    df2['event_time'] = pd.to_datetime(df2['event_time'],
                                                           infer_datetime_format=True)  # ,format='%d%b%Y:%H:%M:%S.%f'
                    df2.event_value.astype('float64')
                    df2.drop_duplicates(subset='event_time', keep='first', inplace=False)
                    df2['event_time'] = df2['event_time'] + timedelta(
                            hours=5)  # En cassandra esta la hora meridiano, se pasa a horalocal.
                    df2.set_index('event_time')
                    
            except:
                df2 = DataFrame()
        else: #Servidor Scada
            try:

                q = """select id_stz, id_measure, date_record, val_data from data_radio.archive_data 
                              where id_stz = %s and id_measure=%s and
                              val_data IS NOT NULL and
                              date_record >= (date_trunc('hour', current_date - interval '%s day'))
                              ORDER BY date_record DESC 
                              limit 5000""" % (ID_STZ_SCADA, sensorS, days_to_subtract)
                consulta=bibi.scadaQuery(q)
                Err = consulta[0]

                if Err == "OK":
                    df2 = consulta[1]
                    qs = df2.shape
                    if qs[0] > 0:
                        df2.columns = ["station", "sensor", "event_time", "event_value"]
                        df2['event_time'] = pd.to_datetime(df2['event_time'],
                                                           infer_datetime_format=True)  # ,format='%d%b%Y:%H:%M:%S.%f'
                        df2.event_value.astype('float64')
                        df2.drop_duplicates(subset='event_time', keep='first', inplace=False)
                        df2.set_index('event_time')
                    else:
                        df2 = DataFrame()
                else:
                    df2 = DataFrame()

            except:
                df2 = DataFrame()
    else:
        df2 = DataFrame()

    return df2
#---PREUEBA DE LA FUNCION----
#estacion ='0012015110'
#ID_STZ_SCADA= -1
#sensorH= '9000'
#sensorS= 8
#days_to_subtract= 1
#servidor='HYDRAS'

#print("NO SE")
#print(SensorQuery(estacion,ID_STZ_SCADA, sensorH, sensorS,days_to_subtract, servidor).head())
#-----------------------------------------------------------------------------------------------------------------------
# Determina el valor minimo promedio de los utimos 30 dias y la tendencia de la bateria de los utimos 30 dias
# Retorna una pareja de valores, donde el primer valor es una badera de alerta y el segundo valor es la tension minima promedio
# de los utimos 30 dias.

def BatCheck(df2):
    #print(df2.shape)
    df2['DailyMin'] = df2.event_value.rolling(24, min_periods=14).min()
    BatMin = round(df2.DailyMin.mean(), 1)  # Valor minimo promedio de los ultimos 28 dias
    dfa = df2.dropna(axis=0, how='any')
    x1 = dfa.index.values
    y1 = dfa.DailyMin.values
    s = x1.shape
    length = s[0]
    #print(length )
    if length >3:
        x = x1.reshape(length, 1)
        y = y1.reshape(length, 1)
        regr = linear_model.LinearRegression()
        regr.fit(x, y)
        b = regr.intercept_
        m = regr.coef_  # Tendiencia mensual de la bateria si pendiente < a -(BatMin-10.9)/4320 => Alarma de descarga
    else:
        m=0
    if BatMin < 11.9:  # El criterio del humbral se debe ajustar
        r = ["BATERIA_BAJA", BatMin]
    elif m < -(BatMin - 10.9) / 4320:
        #print(m,-(BatMin - 10.9) / 4320, BatMin)
        r = ["FALLA:EN_CARGA", BatMin]
    else:
        r = ["OK", BatMin]
    return r


# Funcion para determinar estado de bateria
def BatStatus(CodEstacion, ID_STZ_SCADA, LAST_DATE, servidor):
    sensorH = '9000'
    sensorS = 8
    days_to_subtract = datetime.now() - LAST_DATE
    days_to_subtract = days_to_subtract.days + ventanaObs
    #print(days_to_subtract)
    #try:
    df2 = SensorQuery(CodEstacion, ID_STZ_SCADA, sensorH, sensorS, days_to_subtract, servidor)
    qh = df2.shape
    #print(df2.head())
        
    if qh[0] > 0:
        r = BatCheck(df2)
    else:
        sensorH = '9007'
        df2 = SensorQuery(CodEstacion, ID_STZ_SCADA, sensorH, sensorS, days_to_subtract, servidor)
        qh = df2.shape
        if qh[0] > 0:
            r = BatCheck(df2)
        else:
            r = ["DESCONOCIDO", np.nan]
    #except:
    #    r = ["DESCONOCIDO", np.nan]

    return r

#-- PRUEBA DE LA FUNCION----
#date=datetime.now()# - timedelta(5) - timedelta(days=65)
#print(date)
#print(BatStatus('0012015110', -1,date,'HYDRAS'))
#----------------------------------------------------------------------------------------------------------------------
#-----REVISION ESTADO DE PLUVIOMETRO----
def PluvioStatus(df,**kwargs):
    periodo = 10  # Periodo de medicion en minutos
    dispo_dato=np.nan
    codigo_catalogo = kwargs['codigo_catalogo']
    IDSCADA = kwargs['IDSCADA']
    LAST_DATE = kwargs['LAST_DATE']
    vecinah = kwargs['vecinah']
    D_VECINO = kwargs['D_VECINO']
    servidor = kwargs['servidor']

    x = [np.nan, np.nan, np.nan]
    pliquida = ('0240', 1)
    psolida = ('0234', 23)
    # Busqueda de codigo de scada del vecino
    vecinaS = df[df.CODIGO_CAT == vecinah]
    if vecinaS.shape[0] > 0:
        vecinaS = vecinaS.iloc[0]['ID_STZ_SCADA']
    else:
        vecinaS = -2

    solido = -1

    # Evaluacion de Estado del pluvio-------------------------------------------------------------------

    days_to_subtract = datetime.now() - LAST_DATE
    days_to_subtract = days_to_subtract.days + 2
    df3 = SensorQuery(codigo_catalogo, IDSCADA, pliquida[0], pliquida[1], days_to_subtract, servidor)
    if df3.shape[0] > 0:
        if max(df3['event_time']) < LAST_DATE - timedelta(days=1):
            x[0] = "FUSER"

        else:
            x[0] = "OK"

        solido = 0
    else:
        # days_to_subtract=1
        df3 = SensorQuery(codigo_catalogo, IDSCADA, psolida[0], psolida[1], days_to_subtract, servidor)
        if df3.shape[0] > 0:
            if max(df3['event_time']) < LAST_DATE - timedelta(days=1):
                x[0] = "FUSER"

            else:
                x[0] = "OK"
                # x[1]=df3['event_value'][0]
            solido = 1
        else:
            x[0] = "FUSER"

    days_to_subtract = datetime.now() - LAST_DATE
    days_to_subtract = days_to_subtract.days + ventanaObs # Se define ventana de observacion en dias
    if solido == 0:
        df3 = SensorQuery(codigo_catalogo, IDSCADA, pliquida[0], pliquida[1], days_to_subtract, servidor)
    elif solido == 1:
        df3 = SensorQuery(codigo_catalogo, IDSCADA, psolida[0], psolida[1], days_to_subtract, servidor)
    else:
        df3 = DataFrame()

    # Evaluacion de Disponibilidad------------------------------------------------------------------------
    if df3.shape[0] > 0:
        esperado=((ventanaObs*24*60)//periodo)-1 # Calculo de la cantidad de datos esperados para el peridos de observacion (en dias) segun el periodo de muestreo (en minutos) 
        optenido=df3[df3.event_time >=(datetime.now()- timedelta(days=ventanaObs))]
        optenido = optenido.event_value.count()
        dispo_dato = optenido*100/esperado
        if dispo_dato >100:
            dispo_dato=100
        
    else:
        dispo_dato=0

    # Evaluacion de Intermitencia------------------------------------------------------------------------
    if df3.shape[0] > 0:
        acount = df3.event_value.count()
        intermitencia = 0
        for i in range(0, acount - 1):
            if df3['event_time'][i] > (df3['event_time'][i + 1] + timedelta(minutes=11)):
                intermitencia = intermitencia + 1
            if intermitencia > acount*0.016:  # Si hay mas de 1.6% de missing (huecos) en la serie de datos entonces intermitente
                break

        if x[0] != "FUSER" and intermitencia > acount*0.017:
            x[0] = "INTERMITENTE"
    #---------------------------------------------------------------------------------------------------

    if x[0] != "FUSER" and df3.shape[0] > 0:

        x[1] = round(df3['event_value'].mean(), 1)

        # Evaluacion de datos fuera de rango------------------------------------------------------------

        amin = min(df3['event_value'])
        amax = max(df3['event_value'])
        asum = sum(df3['event_value'])
        if amin < 0 or amax > 400:
            x[0] = "DATOS_ANOMALOS_FUERA_DE_RANGO"

        

        # Evaluacion de posible taponamiento o atascamiento-------------------------------------------------

        elif asum == 0 and D_VECINO < 35:
            df3 = SensorQuery('00' + str(vecinah), vecinaS, pliquida[0], pliquida[1], days_to_subtract, servidor)
            if df3.shape[0] > 0:
                df3 = df3[df3['event_time'] < LAST_DATE]
                if df3.shape[0] > 0:
                    pass
                else:
                    df3 = SensorQuery('00' + str(vecinah), vecinaS, psolida[0], psolida[1], days_to_subtract, servidor)
                    if df3.shape[0] > 0:
                        df3 = df3[df3['event_time'] < LAST_DATE]
            if df3.shape[0] > 0:
                asum = sum(df3['event_value'])
                if asum > 12:
                    x[0] = "PROBABLE_TAPONAMIENTO"
    
       
    x[0] = x[0] +" DISPO "+"{:.2f}".format(dispo_dato)+"%"
    
    return x
#--PRUEBA DE FUNCION----
#kwargs={}
#kwargs['codigo_catalogo'] = '0021185090'
#kwargs['IDSCADA'] = 116
#kwargs['LAST_DATE']=np.nan#datetime.strptime('2018-07-09 14:00:00', "%Y-%m-%d %H:%M:%S")
#kwargs['vecinah']=21235030
#kwargs['D_VECINO']=21.3
#kwargs['servidor']='HYDRAS'

#x=PluvioStatus(df, **kwargs)

#print(x)
#-----------------------------------------------------------------------------------------------------------------------
#--REVISION SENSOR DE NIVEL----
def LevelStatus(df,**kwargs):
    dispo_dato=np.nan
    codigo_catalogo = kwargs['codigo_catalogo']
    IDSCADA = kwargs['IDSCADA']
    LAST_DATE = kwargs['LAST_DATE']
    VECINO_CORREINTE = kwargs['VECINO_CORREINTE']
    D_VECINO_CORRIENTE = kwargs['D_VECINO_CORRIENTE']
    ALTITUD = kwargs['ALTITUD']
    servidor = kwargs['servidor']

    x = ['DESCONOCIDO', np.nan, np.nan]
    s1 = ('0230', 7)  # Nivel Intantaneo rios, lagos, lagunas, quebradas..
    s2 = ('0233', 229)  # Nivel del mar 407
    s3 = ('0407', -1)  # Nivel del mar 407
    periodo = 60  # Periodo de medicion en minutos
    # Busqueda del ID de vecino en scada
    if VECINO_CORREINTE != -1:
        vecinaS = df[df.CODIGO_CAT == VECINO_CORREINTE]

        if vecinaS.shape[0] > 0:
            vecinaS = vecinaS.iloc[0]['ID_STZ_SCADA']
        else:
            vecinaS = -2

    s = -1

    # Evaluacion de Estado del limnimetro------------------------------------------------------------------------------

    days_to_subtract = datetime.now() - LAST_DATE
    days_to_subtract = days_to_subtract.days + 2

    df3 = SensorQuery(codigo_catalogo, IDSCADA, s1[0], s1[1], days_to_subtract, servidor)

    if df3.shape[0] > 0:
        if max(df3['event_time']) < LAST_DATE - timedelta(days=1):
            x[0] = "FUSER"

        else:
            x[0] = "OK"

        s = 0

    else:

        df3 = SensorQuery(codigo_catalogo, IDSCADA, s2[0], s2[1], days_to_subtract, servidor)

        if df3.shape[0] > 0:
            if max(df3['event_time']) < LAST_DATE - timedelta(days=4):
                x[0] = "FUSER"

            else:
                x[0] = "OK"

                # x[1]=df3['event_value'][0]
            s = 1

        else:
            df3 = SensorQuery(codigo_catalogo, IDSCADA, s3[0], s3[1], days_to_subtract, servidor)

            if df3.shape[0] > 0:
                if max(df3['event_time']) < LAST_DATE - timedelta(days=4):
                    x[0] = "FUSER"

                else:
                    x[0] = "OK"

                    # x[1]=df3['event_value'][0]
                s = 2
            else:
                
                x[0] = "FUSER"

    days_to_subtract = datetime.now() - LAST_DATE
    days_to_subtract = days_to_subtract.days + ventanaObs #definicion de la ventana de observacion

    if s == 0:
        df3 = SensorQuery(codigo_catalogo, IDSCADA, s1[0], s1[1], days_to_subtract, servidor)
    elif s == 1:
        df3 = SensorQuery(codigo_catalogo, IDSCADA, s2[0], s2[1], days_to_subtract, servidor)
    elif s==2:
        df3 = SensorQuery(codigo_catalogo, IDSCADA, s3[0], s3[1], days_to_subtract, servidor)
    else:
        df3 = DataFrame()
    #print(df3.head(5))

    # Evaluacion de Disponibilidad------------------------------------------------------------------------
    if df3.shape[0] > 0:
        esperado=((ventanaObs*24*60)//periodo)-1
        optenido=df3[df3.event_time >=(datetime.now()- timedelta(days=ventanaObs))]
        optenido = optenido.event_value.count()
        dispo_dato = optenido*100/esperado
        if dispo_dato >100:
            dispo_dato=100
        
    else:
        dispo_dato=0
        if LAST_DATE == datetime.strptime('1900-01-01 00:00:00', "%Y-%m-%d %H:%M:%S"):
            x[0] = "DESCONOCIDO"
        else:
            x[0] = "FUSER"
    # Evaluacion de Inermitencia-------------------------------------------------------------------------------

    if df3.shape[0] > 0:
        acount = df3.event_value.count()

        intermitencia = 0
        for i in range(0, acount - 1):

            if df3['event_time'][i] > (df3['event_time'][i + 1] + timedelta(minutes=periodo + 11)):
                intermitencia = intermitencia + 1
            if intermitencia > acount*0.016:  # Si hay mas de 4 missing (huecos) en la serie de datos entonces intermitente
                break
        dispo_dato=(1-(intermitencia/(acount+intermitencia+0.000001)))*100 
        if x[0] != "FUSER" and intermitencia > acount*0.016:
            x[0] = "INTERMITENTE"       
    
    if x[0] != "FUSER" and df3.shape[0] > 0:
        
        x[1] = round(df3['event_value'].mean(), 1)

        # Evaluacion de datos fuera de rango------------------------------------------------------------

        amin = df3[df3.event_value < 0]  # min(df3['event_value'])
        amax = df3[df3.event_value > 100]  # max(df3['event_value'])
        # asum=sum(df3['event_value'])
        if amin.shape[0] > 2 or amax.shape[0] > 2:  # Si hay mas de 3 datos almes fuera de Rango, para la variable nivel segun OMM Guia de iNstrumentos y metodos
            x[0] = "DATOS_ANOMALOS_FUERA_DE_RANGO"
        else:
            # Evaluacion de Variabilidad, Maxima 15m/hora ----------------------------------------------------
            if df3.shape[0] > 144:
                df3['Gradient'] = np.gradient(df3.event_value.astype('float'))
                amax = df3[df3.Gradient > 13.5]
                amax = amax.shape[0]
                amax2 = max(df3['Gradient'])
                amin = min(df3['Gradient'])

                if amax > 2 or amax2 > 21 or amin < -6:  # Si hay mas de 3 variaciones anormales al mes
                    x[0] = "DATOS_ANOMALOS_VARIACION_ANORMALMENTE_ALTA"

                else:
                    # Evaluacion de 0 o baja variabilidad-------------------------------------------------

                    df3 = df3.sort_values('event_time', axis=0, ascending=True, inplace=False, kind='quicksort')

                    df3['DailySum'] = df3.Gradient.rolling(144, min_periods=144).sum()

                    dflast = df3[df3.event_time == df3.event_time.max()]
                    dflast2 = df3[df3.event_time == df3.event_time.max() - timedelta(hours=24)]
                    dflast3 = df3[df3.event_time == df3.event_time.max() - timedelta(hours=48)]
                    dflast4 = df3[df3.event_time == df3.event_time.max() - timedelta(hours=72)]
                    dflast5 = df3[df3.event_time == df3.event_time.max() - timedelta(hours=144)]
                    if sum(abs(dflast['DailySum'])) + sum(abs(dflast2['DailySum'])) + sum(
                            abs(dflast3['DailySum'])) + sum(abs(dflast4['DailySum'])) + sum(
                            abs(dflast5['DailySum'])) <= 0.06:
                        x[0] = 'DATOS_ANOMALOS_MUY_BAJA_VARIACION'

        # Evaluacion de correlacion con vecino de corriente La idea es que: *Existe una correlacion entre estaciones
        # en una misma corriente y que una variacion repentina de la correlacion es anomala.

        if VECINO_CORREINTE != -1 and s == 0 and datetime.today().weekday() == 6 : #Solo se calcula los sabados para aligerar el computo

            # Consulta niveles en la estaion vecina y filtrar por rango de tiempo
            df4 = SensorQuery('00' + str(VECINO_CORREINTE), vecinaS, s1[0], s1[1], days_to_subtract, servidor)
            if df4.shape[0] > 10:
                df4 = df4.loc[:, ('event_time', 'event_value')]
                # df4=df4[["event_time","event_value"]]
                df4 = df4[df4.event_time <= df3.event_time.max()]
                df4 = df4[df4.event_time >= df3.event_time.min()]

                df4 = df4.sort_values('event_time', axis=0, ascending=True, inplace=False, kind='quicksort')
                df4.columns = ['event_time', 'V_event_value']

                # Se aplica Filtro pasa bajas T=12periodos y se elimina nivel DC a la vecino
                df4['V_event_value'] = df4.V_event_value.rolling(12, min_periods=12).mean()
                df4['V_event_value'] = df4['V_event_value'] - df4.V_event_value.mean()

                # Se aplica filtro pasa bajas y se elimina nivel DC ala estacion bajo revision
                df3b = df3.loc[:, ('event_time', 'event_value')]
                # df3b=df3[["event_time","event_value"]]
                df3b['event_value'] = df3b.event_value.rolling(12, min_periods=12).mean()
                df3b['event_value'] = df3b['event_value'] - df3b.event_value.mean()

                # Se hacer inner join entre el veciono y la estacion la llave es el event time
                df4 = pd.concat([df3b.set_index('event_time'), df4.set_index('event_time')], axis=1, join='inner')
                # Se eliminan obseraciones con valores nulos.
                df4.dropna(inplace=True)

                a = df4['event_value'].values.tolist()
                b = df4['V_event_value'].values.tolist()
                # Si tenemos mas de 10 observaciones aplicamos correlacion de spearman
                if len(a) > 10:
                    c = np.correlate(a, b, "full")
                    c = np.argmax(c, axis=0)
                    l = len(a)
                    ceros = [0] * (l - 1)
                    a.extend(ceros)
                    b.extend(ceros)
                    ceros.extend(a)
                    a = ceros
                    ceros = [0] * (l - 1)
                    ceros.extend(b)
                    b = ceros
                    a = a[c:2 * l - 1]
                    b = b[l - 1:l + len(a) - 1]
                    correla = stats.spearmanr(a, b)
                    if correla[1] <= 0.05:
                        x[2] = round(correla[0], 2)
                    else:
                        x[2] = 999
                        
    x[0] = x[0] +" DISPO "+"{:.2f}".format(dispo_dato)+"%"
    return x

#--- PREUAB DE LA FUNCION-----
def testLevelStatus():
    #Caso1 
    kwargs={}
    codigo_catalogo = kwargs['codigo_catalogo']= '0026127040'
    IDSCADA = kwargs['IDSCADA'] = 1109
    LAST_DATE = kwargs['LAST_DATE'] = datetime.strptime('2019-05-07 08:00:00', "%Y-%m-%d %H:%M:%S")
    VECINO_CORREINTE = kwargs['VECINO_CORREINTE'] =21
    D_VECINO_CORRIENTE = kwargs['D_VECINO_CORRIENTE']=21
    ALTITUD = kwargs['ALTITUD']=100
    servidor = kwargs['servidor']='HYDRAS'
    x=LevelStatus(**kwargs)
    print('caso1 ',x)
    #Caso2
    codigo_catalogo = kwargs['codigo_catalogo']= '0011159010'
    IDSCADA = kwargs['IDSCADA'] = -1
    LAST_DATE = kwargs['LAST_DATE'] = datetime.strptime('2019-05-16 02:50:00', "%Y-%m-%d %H:%M:%S")
    VECINO_CORREINTE = kwargs['VECINO_CORREINTE'] =21
    D_VECINO_CORRIENTE = kwargs['D_VECINO_CORRIENTE']=21
    ALTITUD = kwargs['ALTITUD']=100
    servidor = kwargs['servidor']='HYDRAS'
    x=LevelStatus(**kwargs)
    print('caso2 ',x)
#testLevelStatus()
#-----------------------------------------------------------------------------------------------------------------------
#--FUNCION PARA REVISAR SENSORES EN GENERAL-------------------------------
def sensorStatus(df,**parametros):
    dispo_dato=np.nan
    codigo_catalogo = parametros['codigo_catalogo']
    IDSCADA = parametros['IDSCADA']
    LAST_DATE = parametros['LAST_DATE']
    vecinah = parametros['vecinah']
    D_VECINO = parametros['D_VECINO']
    servidor = parametros['servidor']
    LLR = parametros['LLR']
    HLR = parametros['HLR']
    DeltaMax = parametros['DeltaMax']
    DeltaMin = parametros['DeltaMin']
    PERIODO = parametros['PERIODO']
    periodo = parametros['periodo']
    s1 = parametros['s1']
    s2 = parametros['s2']

    x = [np.nan, np.nan, np.nan]

    vecinaS = df[df.VECINO == vecinah]  # Correlacion

    if vecinaS.shape[0] > 0:
        vecinaS = vecinaS.iloc[0]['ID_STZ_SCADA']
    else:
        vecinaS = -2
    s = -1

    # Evaluacion de Estado del Sensor-------------------------------------------------------------------

    days_to_subtract = datetime.now() - LAST_DATE
    days_to_subtract = days_to_subtract.days + 2
    #print(codigo_catalogo, ' days_to_subtract:..', days_to_subtract)
    #print((codigo_catalogo, IDSCADA, s1[0], s1[1], days_to_subtract, servidor))
    df3 = SensorQuery(codigo_catalogo, IDSCADA, s1[0], s1[1], days_to_subtract, servidor)
    #print(codigo_catalogo, ' registros: ', df3.shape[0])
    if df3.shape[0] > 0:
        if max(df3['event_time']) < LAST_DATE - timedelta(days=1):
            x[0] = "FUSER"
            # print('fuser porque mas de un dia de retraso')

        else:
            x[0] = "OK"
            # x[1]=df3['event_value'][0]
        s = 0
    else:
        # days_to_subtract=1
        # print('no lo encotro con el codigo original')
        df3 = SensorQuery(codigo_catalogo, IDSCADA, s2[0], s2[1], days_to_subtract, servidor)
        #print(codigo_catalogo, ' registros: ', df3.shape[0])
        if df3.shape[0] > 0:
            if max(df3['event_time']) < LAST_DATE - timedelta(days=1):
                x[0] = "FUSER"
                # print('fuser porque mas de un dia de retraso codigo2')
            else:
                x[0] = "OK"
            s = 1
        else:
            x[0] = "FUSER"
            # print('fuser por qeu no lo encontro')

    days_to_subtract = datetime.now() - LAST_DATE
    days_to_subtract = days_to_subtract.days + ventanaObs #definicion de la ventana de observacion

    if s == 0:
        df3 = SensorQuery(codigo_catalogo, IDSCADA, s1[0], s1[1], days_to_subtract, servidor)
    elif s == 1:
        df3 = SensorQuery(codigo_catalogo, IDSCADA, s2[0], s2[1], days_to_subtract, servidor)
    else:
        df3 = DataFrame()

    
    # Evaluacion de Disponibilidad------------------------------------------------------------------------
    if df3.shape[0] > 0:
        esperado=((ventanaObs*24*60)//periodo)-1
        optenido=df3[df3.event_time >=(datetime.now()- timedelta(days=ventanaObs))]
        optenido = optenido.event_value.count()
        dispo_dato = optenido*100/esperado
        if dispo_dato >100:
            dispo_dato=100
        
    else:
        dispo_dato=0
        if LAST_DATE == datetime.strptime('1900-01-01 00:00:00', "%Y-%m-%d %H:%M:%S"):
            x[0] = "DESCONOCIDO"
        else:
            x[0] = "FUSER"
    # Evaluacion de Inermitencia------------------------------------------------------------------------
    
    if df3.shape[0] > 0:
        acount = df3.event_value.count()
        intermitencia = 0
        if acount>0:
            acount= acount-1
        else:
            pass
        for i in range(0, acount):
            if df3['event_time'][i] > (df3['event_time'][i + 1] + timedelta(minutes=periodo +11)):
                intermitencia = intermitencia + 1
            
            if intermitencia > acount*0.016:  # Si hay mas de 1.6% de missing (huecos) en la serie de datos entonces intermitente
                break
            
        if x[0] != "FUSER" and intermitencia > acount*0.016:
            x[0] = "INTERMITENTE"
        x[1] = round(df3['event_value'].mean(), 1)
    
            
    # Evaluacion ------------------------------------------------------------------------------
    if x[0] != "FUSER" and df3.shape[0] > 0:
        amin = df3[df3.event_value < LLR]  # min(df3['event_value'])
        amax = df3[df3.event_value > HLR]  # max(df3['event_value'])
        if amin.shape[0] > 2 or amax.shape[0] > 2:
            x[0] = "DATOS_ANOMALOS_FUERA_DE_RANGO"
        else:
            # Evaluacion de Variabilidad, Maxima  ----------------------------------------------------
            try:
                df3['Gradient'] = np.gradient(df3.event_value.astype('float'))
                Delta = df3['Gradient'].abs()
                amax = df3[df3.Gradient.abs() > DeltaMax]
                amax = amax.shape[0]
                amax2 = max(Delta)
                    # a3Q=Delta.quantile(0.75)
                    # a1Q=Delta.quantile(0.25)
                    # Outlier=a3Q+1.5*(a3Q-a1Q)
                    # print('Outlier ',Outlier)

                if amax > 3 or amax2 > DeltaMax * 2:  # Si hay mas de 3 variaciones anormales al mes
                    x[0] = "DATOS_ANOMALOS_VARIACION_ANORMALMENTE_ALTA"

                else:
                        # Evaluacion de 0 o baja variabilidad-------------------------------------------------

                    df3 = df3.sort_values('event_time', axis=0, ascending=True, inplace=False, kind='quicksort')

                    df3['DailySum'] = df3.Gradient.abs().rolling(PERIODO, min_periods=PERIODO).sum()
                    if max(df3['DailySum']) < DeltaMin:
                        x[0] = 'DATOS_ANOMALOS_MUY_BAJA_VARIACION'
                    else:
                        # Evaluacion de correlacion con vecino de corriente La idea es que: *Existe una correlacion entre estaciones
                        # en una misma corriente y que una variacion repentina de la correlacion es anomala.

                        if vecinah != -1 and D_VECINO < 40 and datetime.today().weekday() == 6 :

                            # Consulta niveles en la estaion vecina y filtrar por rango de tiempo
                            df4 = SensorQuery('00' + str(vecinah), vecinaS, s1[0], s1[1], days_to_subtract,
                                                  servidor)
                            if df4.shape[0] < 10:
                                df4 = SensorQuery('00' + str(vecinah), vecinaS, s2[0], s2[1], days_to_subtract,
                                                      servidor)
                            if df4.shape[0] > 10:
                                df4 = df4.loc[:, ('event_time', 'event_value')]
                                # df4[["event_time","event_value"]]
                                df4 = df4[df4.event_time <= df3.event_time.max()]
                                df4 = df4[df4.event_time >= df3.event_time.min()]
                                df4 = df4.sort_values('event_time', axis=0, ascending=True, inplace=False,
                                                          kind='quicksort')
                                df4.columns = ['event_time', 'V_event_value']

                                # Se aplica Filtro pasa bajas T=12periodos y se elimina nivel DC a la vecino
                                df4['V_event_value'] = df4.V_event_value.rolling(int(PERIODO / 1), min_periods=int(
                                        PERIODO / 1)).mean()
                                df4['V_event_value'] = df4['V_event_value'] - df4.V_event_value.mean()
                                # Se aplica filtro pasa bajas y se elimina nivel DC ala estacion bajo revision
                                df3b = df3.loc[:, ('event_time', 'event_value')]
                                # df3b=df3[["event_time","event_value"]]
                                df3b['event_value'] = df3b.event_value.rolling(int(PERIODO / 1),
                                                                                   min_periods=int(PERIODO / 1)).mean()
                                df3b['event_value'] = df3b['event_value'] - df3b.event_value.mean()

                                # Se hacer inner join entre el veciono y la estacion la lave es el event time
                                df4 = pd.concat([df3b.set_index('event_time'), df4.set_index('event_time')], axis=1,
                                                    join='inner')
                                # Se eliminan obseraciones con valores nulos.
                                df4.dropna(inplace=True)
                                a = df4['event_value'].values.tolist()
                                b = df4['V_event_value'].values.tolist()
                                # Si tenemos mas de 10 observaciones aplicamos correlacion de spearman
                                if len(a) > 10:
                                    try:
                                        correla = stats.spearmanr(a, b)
                                    except:
                                        correla = np.nan

                                    if correla[1] <= 0.05:
                                        x[2] = round(correla[0], 2)
                                    else:
                                        x[2] = 999
            except:
                pass
    x[0] = x[0] +" DISPO "+"{:.2f}".format(dispo_dato)+"%"

    return x



##--PRUEBA DE FUNCION----
#parametrosE={'codigo_catalogo' : '0021097070','IDSCADA' : -1,'DCP_Address': 'ag','vecinah':np.nan,'D_VECINO':1,'VECINO_CORREINTE':'','D_VECINO_CORRIENTE':1,'ALTITUD':2,'servidor':'SCADA', 'mediciones':[2],'LAST_DATE':datetime.now()}
#parametrosV={}
#parametrosV['s1']=('0230',2)#('0103',2)#('0237',12)
#parametrosV['s2']=(np.nan,np.nan)
#parametrosV['LLR']= 0
#parametrosV['HLR']=45
#parametrosV['DeltaMax']=20
#parametrosV['DeltaMin']=2
#parametrosV['PERIODO']=24
#parametrosV['periodo']=10
#parametros=dict(parametrosE, **parametrosV)
#print(sensorStatus(**parametros))
#-----------------------------------------------------------------------------------------------------------------------
