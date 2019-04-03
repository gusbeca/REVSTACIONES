import rojillas
import bibi
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
import time
import pymysql
import sqlalchemy
import mysql.connector
from mysql.connector import errorcode  #Librerias para conexion a BD mysql para guradar resultados
import sqlalchemy
import math

def revisionVariable(var, codigos, i, parametros, df):
    
    if any({*codigos} & {*parametros['mediciones']}):
        if var[1] == 'Precipitacion':
            x = rojillas.PluvioStatus(df,
                **parametros)  # codigo_catalogo, IDSCADA,LAST_DATE,vecinah,D_VECINO,servidor)
        elif var[1] == 'Nivel':
            x = rojillas.LevelStatus(df,**parametros)
        else:
            x = rojillas.sensorStatus(df,**parametros)

        if x:
            pass
        else:
            x = [np.nan, np.nan, np.nan]
        df.iloc[i, df.columns.get_loc(var[0])] = x[2]
        df.iloc[i, df.columns.get_loc(var[1])] = x[1]
        df.iloc[i, df.columns.get_loc(var[2])] = x[0]

    elif len(parametros['mediciones']) == 0:
        for each in var: df.iloc[i, df.columns.get_loc(each)] = np.nan
    else:
        for each in var: df.iloc[i, df.columns.get_loc(each)] = np.nan


def isNaN(x):
    a= False
    try:
        a= x != x 
    except:
        a= False
    if a== False:
        try:
            a= x is np.nan
        except:
            a= False
    if a== False:
        
        try:
            a= math.isnan(x)
        except:
            a= False
    if a==False: 
        try:
            a == np.datetime64('NaT')
        except:
            a= False
        
    return a

#def revisionEstacion:
    

def revisarRed(inicio, fin, df):
    start = time.time()
    q = """select id_stz from data_radio.archive_data 
               limit 1"""
    consulta= bibi.scadaQ(q)
    #print(consulta[0])
    if consulta[0]== 'OK':
        for i in range(inicio, fin): 
                # --------------------------------------------------------------------------------------------------------------------

                parametrosE = {'codigo_catalogo': "00" + str(df.iloc[i]['CODIGO_CAT']),
                               'IDSCADA': int(df.iloc[i]['ID_STZ_SCADA']),
                               'DCP_Address': df.iloc[i]['DCP_ADDRESS'],
                               'vecinah': df.iloc[i]['VECINO'],
                               'D_VECINO': df.iloc[i]['D_VECINO'],
                               'VECINO_CORREINTE': df.iloc[i]['VECINO_CORREINTE'],
                               'D_VECINO_CORRIENTE': df.iloc[i]['D_VECINO_CORRIENTE'],
                               'ALTITUD': df.iloc[i]['ALTITUD'],
                               'servidor': '', 'mediciones': [], 'LAST_DATE': ''}

                # ---------------------------Verificacion de ESTADO en Servidores----------------------------------------------------

                x = rojillas.StateQuery(**parametrosE)
                
                df.iloc[i, df.columns.get_loc('SERVIDOR')] = x[0]
                df.iloc[i, df.columns.get_loc('LAST_DATE')] = x[1]
                df.iloc[i, df.columns.get_loc('ON_LINE')] = x[3]

                print('CODIGO_CAT: ',str(df.iloc[i]['CODIGO_CAT']), ' LAST_DATE: ', x[1], ' ON_LINE: ', x[3],' SERVIDOR: ', x[0])

                parametrosE['servidor'] = x[0]
                parametrosE['mediciones'] = x[2]  # Lista con las mediciones de la estacion
                parametrosE['LAST_DATE'] = x[1]  # Fecha ultimo dato en servidor
                
                if not (isNaN(x[1])):
                    
                    # --------------------------Verificacion GOES------------------------------------------------------------------------------

                    if parametrosE['DCP_Address'] != -1:
                        x = rojillas.dcpmon(**parametrosE)
                        df.iloc[i, df.columns.get_loc('NOAA_INMARSAT')] = x[0]
                        df.iloc[i, df.columns.get_loc('GananciaAntena')] = x[1]

                    # ----------------------------Revision Precipitacion---------------------------------------------------------------------

                    var = ['CorrelaPrecipitacion', 'Precipitacion', 'EstadoPluvio']
                    codigos = [1, 23, '0234', '0240']
                    revisionVariable(var, codigos, i, parametrosE,df)

                    # ---------------------------Revision Nivel--------------------------------------------------------------------------------
                    # Si es hydrologica tiene que revisar nivel
                    codigos = [7, '0230', '0407']
                    if (df.iloc[i]['CLASE'] == 'HID' or df.iloc[i]['CLASE'] == 'HMT') and (df.iloc[i]['CATEG']== 'LG' or df.iloc[i]['CATEG']== 'LM' or df.iloc[i]['CATEG']== 'MM'):
                        if any({*codigos} & {*parametrosE['mediciones']}):
                            pass
                        elif (df.iloc[i]['CATEG']== 'LG' or df.iloc[i]['CATEG']== 'LM'):
                            try:
                                parametrosE['mediciones'].append(7)
                                parametrosE['mediciones'].append('0230')
                            except:
                                pass
                        elif (df.iloc[i]['CATEG']== 'MM'):
                            
                            try:
                                parametrosE['mediciones'].append(7)
                                parametrosE['mediciones'].append('0240')
                            except:
                                pass
                            
                    var = ['CorrelaNivel', 'Nivel', 'EstadoNivel']
                    
                    revisionVariable(var, codigos, i, parametrosE,df)

                    # ---------------------------Revision Temperatura del Aire a 2m---------------------------------------------------------

                    var = ['CorrelaTempAire2m', 'TempAire2m', 'EstadoTempAire2m']
                    codigos = [5, '0068']
                    parametrosV = {'LLR': -25, 'HLR': 60, 'DeltaMax': 8, 'DeltaMin': 3, 'PERIODO': 24, 'periodo': 60,
                                   's1': ('0068', 5), 's2': (np.nan, np.nan)}
                    parametros = dict(parametrosE, **parametrosV)
                    revisionVariable(var, codigos, i, parametros,df)

                    # ---------------------Revision Temperatura del Aire a 10cm--------------------------------------------------------------------

                    codigos = [21, '0075']
                    var = ['CorrelaTempAire10cm', 'TempAire10cm', 'EstadoTempAire10cm']
                    parametrosV = {'LLR': -25, 'HLR': 60, 'DeltaMax': 8, 'DeltaMin': 3, 'periodo': 60, 'PERIODO': 24,
                                   's1': ('0075', 21), 's2': (np.nan, np.nan)}
                    parametros = dict(parametrosE, **parametrosV)
                    revisionVariable(var, codigos, i, parametros,df)

                    # ------------------------ Revision Humedad del Aire a 2m-----------------------------------------------------------------------
                    codigos = [6, '0027', '0028']
                    var = ['CorrelaHumAire2m', 'HumAire2m', 'EstadoHumAire2m']
                    parametrosV = {'LLR': 0, 'HLR': 100, 'DeltaMax': 35, 'DeltaMin': 10, 'periodo': 60, 'PERIODO': 24,
                                   's1': ('0027', 6), 's2': ('0028', np.nan)}
                    parametros = dict(parametrosE, **parametrosV)
                    revisionVariable(var, codigos, i, parametros,df)

                    # ------------------------- Revision Humedad del Aire a 10cm-----------------------------------------------------------------------
                    codigos = [20, '0030']
                    var = ['CorrelaHumAire10cm', 'HumAire10cm', 'EstadoHumAire10cm']
                    parametrosV = {'LLR': 0, 'HLR': 100, 'DeltaMax': 35, 'DeltaMin': 10, 'periodo': 60, 'PERIODO': 24,
                                   's1': ('0030', 20), 's2': (np.nan, np.nan)}
                    parametros = dict(parametrosE, **parametrosV)
                    revisionVariable(var, codigos, i, parametros,df)

                    # ------------------------Revision Velocidad del Viento Anemometro--------------------------------------------------------------

                    codigos = [2, '0103', 90]
                    var = ['CorrelaVelViento', 'VelViento', 'EstadoVelViento']
                    #print(parametrosE)
                    parametrosV = {'LLR': 0, 'HLR': 75, 'DeltaMax': 35, 'DeltaMin': 10, 'periodo': 10, 'PERIODO': 24,
                                   's1': ('0103', 2), 's2': ('0103', 90)}
                    parametros = dict(parametrosE, **parametrosV)
                    revisionVariable(var, codigos, i, parametros,df)

                    # ------------------------Revision Direccion del Viento Anemometro-------------------------------------------------------------------

                    codigos = [3, '0104', '0320', 91]
                    var = ['CorrelaDirViento', 'DirViento', 'EstadoDirViento']
                    parametrosV = {'LLR': 0, 'HLR': 365, 'DeltaMax': 365, 'DeltaMin': 10, 'periodo': 10, 'PERIODO': 24,
                                   's1': ('0104', 3), 's2': ('0320', 91)}
                    parametros = dict(parametrosE, **parametrosV)
                    revisionVariable(var, codigos, i, parametros,df)

                    # ------------------------Revision Radiacion Global Piranometro-----------------------------------------------------------------------

                    codigos = [12, '0236', '0239']
                    var = ['CorrelaRadiacionG', 'RadGlobal', 'EstadoRadicionG']
                    parametrosV = {'LLR': -10, 'HLR': 1200, 'DeltaMax': 800, 'DeltaMin': 3, 'periodo': 60, 'PERIODO': 24,
                                   's1': ('0236', 12), 's2': ('0239', -1)}
                    parametros = dict(parametrosE, **parametrosV)
                    revisionVariable(var, codigos, i, parametros,df)

                    # ------------------------Radiacion Visible--------------------------------------------------------------------------------------

                    codigos = [11, '0237']
                    var = ['CorrelaRadiacionV', 'RadVisible', 'EstadoRadiacionV']
                    parametrosV = {'LLR': -10, 'HLR': 2200, 'DeltaMax': 1600, 'DeltaMin': 6, 'periodo': 60, 'PERIODO': 24,
                                   's1': ('0237', 11), 's2': ('-1', -1)}
                    parametros = dict(parametrosE, **parametrosV)
                    revisionVariable(var, codigos, i, parametros,df)

                    # ------------------------Revision Presion Atmosferica Barometro-----------------------------------------------------------------------

                    codigos = [4, '0255']
                    var = ['CorreralPresionAtm', 'PresionAtmosferica', 'EstadoPresionAtm']
                    parametrosV = {'LLR': 500, 'HLR': 1100, 'DeltaMax': 40, 'DeltaMin': 2, 'periodo': 60, 'PERIODO': 24,
                                   's1': ('0255', 4), 's2': ('-1', -1)}
                    parametros = dict(parametrosE, **parametrosV)
                    revisionVariable(var, codigos, i, parametros,df)

                    # ------------------------Revision Evaporacion-----------------------------------------------------------------------

                    codigos = [13, '0260']
                    var = ['CorrelaEvaporacion', 'Evaporacion', 'EstadoEvaporacion']
                    parametrosV = {'LLR': 0, 'HLR': 180, 'DeltaMax': 150, 'DeltaMin': 2, 'periodo': 60, 'PERIODO': 72,
                                   's1': ('0260', 13), 's2': ('-1', -1)}
                    parametros = dict(parametrosE, **parametrosV)
                    revisionVariable(var, codigos, i, parametros,df)

                    # ------------------------Revision Humead del Suelo <=20cm-----------------------------------------------------------------------

                    codigos = [15, 25, '0245', '0263']
                    var = ['CorrelaHumaS20', 'HumaS20', 'EstadoHumaS20']
                    parametrosV = {'LLR': 0, 'HLR': 100, 'DeltaMax': 100, 'DeltaMin': 3, 'periodo': 60, 'PERIODO': 72,
                                   's1': ('0245', 15), 's2': ('0263', 25)}
                    parametros = dict(parametrosE, **parametrosV)
                    revisionVariable(var, codigos, i, parametros,df)

                    # ------------------------Revision Humead del Suelo =30cm-----------------------------------------------------------------------

                    codigos = [10, '0246']
                    var = ['CorrelaHumaS30', 'HumaS30', 'EstadoHumaS30']
                    parametrosV = {'LLR': 0, 'HLR': 100, 'DeltaMax': 100, 'DeltaMin': 3, 'periodo': 60, 'PERIODO': 72,
                                   's1': ('0246', 10), 's2': (-1, -1)}
                    parametros = dict(parametrosE, **parametrosV)
                    revisionVariable(var, codigos, i, parametros,df)

                    # ------------------------Revision Humead del Suelo 50-100cm-----------------------------------------------------------------------

                    codigos = [16, 27, '0247', '0269']
                    var = ['CorrelaHumaS100', 'HumaS100', 'EstadoHumaS100']
                    parametrosV = {'LLR': 0, 'HLR': 100, 'DeltaMax': 100, 'DeltaMin': 3, 'periodo': 60, 'PERIODO': 72,
                                   's1': ('0247', 16), 's2': ('0269', -1)}
                    parametros = dict(parametrosE, **parametrosV)
                    revisionVariable(var, codigos, i, parametros,df)

                    # ------------------------Revision Temperatura del Suelo <=20cm-----------------------------------------------------------------------

                    codigos = [9, 26, '0241', '0264']
                    var = ['CorrelaTempS20', 'TempS20', 'EstadTempS20']
                    parametrosV = {'LLR': -15, 'HLR': 50, 'DeltaMax': 8, 'DeltaMin': 3, 'periodo': 60, 'PERIODO': 24,
                                   's1': ('0241', 9), 's2': ('0264', 14)}
                    parametros = dict(parametrosE, **parametrosV)
                    revisionVariable(var, codigos, i, parametros,df)

                    # ------------------------Revision Temperatura del Suelo =30cm-----------------------------------------------------------------------

                    codigos = [14, 19, '0242']
                    var = ['CorrelaTempS50', 'TempS50', 'EstadoTempS50']
                    parametrosV = {'LLR': -15, 'HLR': 50, 'DeltaMax': 5, 'DeltaMin': 2, 'periodo': 60, 'PERIODO': 24,
                                   's1': ('0242', 14), 's2': (-1, 19)}
                    parametros = dict(parametrosE, **parametrosV)
                    revisionVariable(var, codigos, i, parametros,df)

                    # ------------------------Revision Temperatura del Suelo 50-100cm-----------------------------------------------------------------------

                    codigos = [9, '0243']
                    var = ['CorrelaTempS100', 'TempS100', 'EstadoTempS100']
                    parametrosV = {'LLR': -15, 'HLR': 50, 'DeltaMax': 4, 'DeltaMin': 2, 'periodo': 60, 'PERIODO': 24,
                                   's1': ('0243', 9), 's2': (-1, -1)}
                    parametros = dict(parametrosE, **parametrosV)
                    revisionVariable(var, codigos, i, parametros,df)

                    # -------------------------Revision de Bateria------------------------------------------------------------------

                    x = rojillas.BatStatus(parametrosE['codigo_catalogo'], parametrosE['IDSCADA'], parametrosE['LAST_DATE'],
                                  parametrosE['servidor'])
                    df.iloc[i, df.columns.get_loc('EstBateria')] = x[0]
                    df.iloc[i, df.columns.get_loc('Bateria')] = x[1]
                    
            # ---------------------------PUT TIME STAMP--------------------------------------------------------------------
                df.iloc[i, df.columns.get_loc('FECHA_REVISION')] = datetime.now()
                if i % 10 == 0:
                    print(round(i * 100 / (fin-inicio), 0), " %------ tiempo transcurrido: ", (time.time() - start) / 60)
  
        df['FECHA_REVISION'] = pd.to_datetime(df.FECHA_REVISION, infer_datetime_format=True)
        df['FECHA_INST'] = pd.to_datetime(df.FECHA_INST, infer_datetime_format=True)
        df['FECHA_SUSP'] = pd.to_datetime(df.FECHA_SUSP, infer_datetime_format=True)
        df['LAST_DATE'] = pd.to_datetime(df.LAST_DATE, infer_datetime_format=True)
        try:
            df = df.drop(['Estatus0_100'], axis=1)
        except:
            print('[Estatus0_100] not found in axis')
        #----------------------Guardar  revision en archivo de texto-------------------------------------------------------
        file_name = 'C:/Python/Python36/RevEstacionesV0.1/estadoRedTes3.csv'
        df.to_csv(file_name, sep=',', encoding='utf-8')
        file_name = 'C:/Windows/System32/estadoRedTes3.csv'
        df.to_csv(file_name, sep=',', encoding='utf-8')
        #-------------------GUARDAR REVISION EN BD LOCAL MYSQL--------------------------------------------------------------
        df = df.loc[:,~df.columns.duplicated()]
        dfRevisiones=df.iloc[:,25:]
        campos = df.columns.values.tolist()
        aT=campos.index("LAST_DATE")
        #print(campos)
        campos2 = ['CODIGO_CAT', 'FECHA_REVISION'] + campos[aT:]

        #print(campos2)

        dfrevisiones = df[campos2]

        pymysql.converters.encoders[np.float64] = pymysql.converters.escape_float
        pymysql.converters.conversions = pymysql.converters.encoders.copy()
        pymysql.converters.conversions.update(pymysql.converters.decoders)


        engine = sqlalchemy.create_engine('mysql+pymysql://root:ideam@localhost:3306/automatizacion')
        #print(dfrevisiones['CODIGO_CAT'])
        dfrevisiones.to_sql(name='revision', con=engine, index=False, if_exists='append')  # Popular tabla revisiones

        #print("termino de revisar y guaro0)")

        #-----SACAR RESUMEN PARA EL JEFE-----------------------------------------------------------------------------------
        
        query = """SELECT FECHA_REVISION, NOMBRE, estaciones.CODIGO_CAT, AREA_OPERATIVA, CLASE, CATEG,
                                     CORRIENTE, DEPTO, LAST_DATE,  ON_LINE,
                                     CONCAT(IF((EstadoTempAire2m IS NULL ),'', CONCAT(' TempA2m: ', EstadoTempAire2m)),
                                     IF((EstadoTempAire10cm IS NULL ),'',CONCAT(', TempA10cm: ', EstadoTempAire10cm)),
                                     IF((EstadTempS20 IS NULL ),'',CONCAT(', TempS20: ', EstadTempS20)),
                                     IF((EstadoTempS50 IS NULL ),'',CONCAT(', TempS50: ', EstadoTempS50)),
                                     IF((EstadoTempS100 IS NULL ),'',CONCAT(', TempS100: ', EstadoTempS100)),
                                     IF((EstadoHumAire2m IS NULL ),'',CONCAT(', HumA2m: ', EstadoHumAire2m)),
                                     IF((EstadoHumAire10cm IS NULL ),'',CONCAT(', HumA10cm: ', EstadoHumAire10cm)),
                                     IF((EstadoHumaS20 IS NULL ),'',CONCAT(', HumS20: ', EstadoHumaS20)),
                                     IF((EstadoHumaS30 IS NULL ),'',CONCAT(', HumS30: ', EstadoHumaS30)),
                                     IF((EstadoHumaS100 IS NULL ),'',CONCAT(', HumS100: ', EstadoHumaS100)),
                                     IF((EstadoRadicionG IS NULL ),'',CONCAT(', RadG: ', EstadoRadicionG)),
                                     IF((EstadoRadiacionV IS NULL ),'',CONCAT(', RadV: ', EstadoRadiacionV)),
                                     IF((EstadoEvaporacion IS NULL ),'',CONCAT(', Evaprcn: ', EstadoEvaporacion)),
                                     IF((EstadoPresionAtm IS NULL ),'',CONCAT(', PresAtm: ', EstadoPresionAtm)),
                                     IF((EstadoVelViento IS NULL ),'',CONCAT(', VelVnt: ', EstadoVelViento)),
                                     IF((EstadoPluvio IS NULL ),'',CONCAT(', Plvio: ', EstadoPluvio)),
                                     IF((EstadoNivel IS NULL ),'',CONCAT(', Nvl: ', EstadoNivel)),
                                     IF((EstBateria IS NULL OR EstBateria='DESCONOCIDO'),', Btria: DESCONOCIDO',CONCAT(', Btria: ', EstBateria)),
                                     IF((NOAA_INMARSAT IS NULL OR NOAA_INMARSAT='DESCONOCIDO'),'',CONCAT(', GOES: ',NOAA_INMARSAT))
                                     ) as COMENTARIOS
                         FROM estaciones 
                         LEFT JOIN revision ON estaciones.CODIGO_CAT = revision.CODIGO_CAT
                         WHERE  DATE(revision.FECHA_REVISION) = CURDATE()"""
        dfResumen = pd.read_sql_query(query, engine)
        file_name = 'C:/Python/Python36/RevEstacionesV0.1/ResumenEstR.csv'
        dfResumen.to_csv(file_name, sep=',', encoding='utf-8')
        
    else:
        print('Error en conexion a BS Scada')
