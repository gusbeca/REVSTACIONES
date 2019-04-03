import pandas as pd
import mysql.connector
from mysql.connector import errorcode  #Librerias para conexion a BD mysql para guradar resultados
import sqlalchemy
import pymysql
import numpy as np

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

  q = """CREATE TABLE estaciones (  CODIGO_CAT INT NOT NULL PRIMARY KEY,
                                  AREA_OPERATIVA INT NOT NULL, 
                                  NOMBRE VARCHAR(64),
                                  TIPO   VARCHAR(10),
                                  CLASE  VARCHAR(10),
                                  CATEG  VARCHAR(10),
                                  ESTADO_CAT VARCHAR(10),
                                  CORRIENTE  VARCHAR(64),
                                  DEPTO VARCHAR(64),
                                  MPIO VARCHAR(64),
                                  LATITUD DOUBLE,
                                  LONGITUD DOUBLE,
                                  ALTITUD  INT,
                                  FECHA_INST DATE,
                                  FECHA_SUSP DATE,
                                  ID_STZ_SCADA INT,
                                  DCP_ADDRESS VARCHAR(12),
                                  ESTADO2  VARCHAR(20),
                                  TRANSMISION VARCHAR(12),
                                  SINIESTRO INT,
                                  VECINO INT,
                                  D_VECINO DOUBLE,
                                  VECINO_CORREINTE INT,
                                  D_VECINO_CORRIENTE INT,
                                  TAir2m VARCHAR(2),
                                  TAir10cm VARCHAR(2),
                                  TS10cm VARCHAR(2),
                                  TS30cm VARCHAR(2),
                                  TS50cm VARCHAR(2),
                                  HRAire2m VARCHAR(2),
                                  HRAire10cm VARCHAR(2),
                                  HRSueloCorto VARCHAR(2),
                                  HRSueloMedio VARCHAR(2),
                                  HRSueloLargo VARCHAR(2),
                                  RadG VARCHAR(2),
                                  RadV VARCHAR(2),
                                  Evap VARCHAR(2,
                                  PresAtm VARCHAR(2),
                                  VelVi VARCHAR(2),
                                  DirVi VARCHAR(2),
                                  Prcptcn VARCHAR(2),
                                  Nvl VARCHAR(2)
                                  )"""

  mySQLCursor.execute(q)

  q2 = """CREATE TABLE revision (id INT AUTO_INCREMENT PRIMARY KEY,
                                  FECHA_REVISION DATETIME,
                                  CODIGO_CAT INT NOT NULL,
                                  INDEX par_ind (CODIGO_CAT),
                                  LAST_DATE DATETIME,
                                  ON_LINE VARCHAR(12),
                                  SERVIDOR VARCHAR(12),
                                  GananciaAntena DOUBLE,
                                  AlertaGananciaAntenaBajando DOUBLE,
                                  NOAA_INMARSAT VARCHAR(100),
                                  CorrelaTempAire2m FLOAT,
                                  EstadoTempAire2m VARCHAR(100),
                                  TempAire2m FLOAT,
                                  CorrelaTempAire10cm FLOAT,
                                  EstadoTempAire10cm VARCHAR(100),
                                  TempAire10cm FLOAT,
                                  CorrelaTempS20 FLOAT,
                                  TempS20 FLOAT,
                                  EstadTempS20 VARCHAR(100),
                                  CorrelaTempS50 FLOAT,
                                  TempS50 FLOAT,
                                  EstadoTempS50 VARCHAR(100),
                                  CorrelaTempS100 FLOAT,
                                  TempS100 FLOAT,
                                  EstadoTempS100 VARCHAR(100),
                                  EstadoHumAire2m VARCHAR(100),
                                  CorrelaHumAire2m FLOAT,
                                  HumAire2m FLOAT,
                                  EstadoHumAire10cm VARCHAR(100),
                                  CorrelaHumAire10cm FLOAT,
                                  HumAire10cm FLOAT,
                                  CorrelaHumaS20 FLOAT,
                                  EstadoHumaS20  VARCHAR(100),
                                  HumaS20  FLOAT,
                                  HumaS30 FLOAT,
                                  CorrelaHumaS30 FLOAT,
                                  EstadoHumaS30 VARCHAR(100),
                                  CorrelaHumaS100 FLOAT,
                                  HumaS100 FLOAT,
                                  EstadoHumaS100 VARCHAR(100),
                                  CorrelaRadiacionG FLOAT,
                                  EstadoRadicionG VARCHAR(100),
                                  RadGlobal FLOAT,
                                  CorrelaRadiacionV FLOAT,
                                  EstadoRadiacionV VARCHAR(100),
                                  RadVisible FLOAT,
                                  CorrelaEvaporacion FLOAT,
                                  EstadoEvaporacion VARCHAR(100),
                                  Evaporacion  FLOAT,
                                  CorreralPresionAtm FLOAT,
                                  EstadoPresionAtm VARCHAR(100),
                                  PresionAtmosferica FLOAT,
                                  EstadoVelViento VARCHAR(100),
                                  VelViento FLOAT,
                                  CorrelaVelViento FLOAT,
                                  EstadoDirViento VARCHAR(100),
                                  CorrelaDirViento FLOAT,
                                  DirViento  FLOAT,
                                  EstadoPluvio VARCHAR(100),
                                  CorrelaPrecipitacion FLOAT,
                                  Precipitacion FLOAT,
                                  CorrelaNivel FLOAT,
                                  EstadoNivel VARCHAR(100),
                                  Nivel FLOAT,
                                  EstBateria  VARCHAR(100),
                                  Bateria FLOAT,
                                  Estatus0_100 FLOAT AS 
                                   (

                                       (
                                       IF((EstadoTempAire2m IS NOT NULL AND  EstadoTempAire2m !='FUSER'),1, 0)+
                                       IF((EstadoTempAire10cm IS NOT NULL AND EstadoTempAire10cm !='FUSER'),1,0)+
                                       IF((EstadTempS20 IS NOT NULL AND EstadTempS20 !='FUSER'),1,0)+
                                       IF((EstadoTempS50 IS NOT NULL AND EstadoTempS50 !='FUSER'),1,0)+
                                       IF((EstadoTempS100 IS NOT NULL AND EstadoTempS100 !='FUSER'),1,0)+
                                       IF((EstadoHumAire2m IS  NOT NULL AND EstadoHumAire2m !='FUSER'),1,0)+
                                       IF((EstadoHumAire10cm IS NOT NULL AND EstadoHumAire10cm !='FUSER'),1,0)+
                                       IF((EstadoHumaS20 IS NOT NULL AND EstadoHumaS20 !='FUSER'),1,0)+
                                       IF((EstadoHumaS30 IS NOT NULL AND  EstadoHumaS30 !='FUSER'),1,0)+
                                       IF((EstadoHumaS100 IS NOT NULL AND EstadoHumaS100 !='FUSER'),1,0)+
                                       IF((EstadoRadicionG IS NOT NULL AND  EstadoRadicionG !='FUSER'),1,0)+
                                       IF((EstadoRadiacionV IS NOT NULL AND EstadoRadiacionV !='FUSER'),1,0)+
                                       IF((EstadoEvaporacion IS NOT NULL AND EstadoEvaporacion !='FUSER'),1,0)+
                                       IF((EstadoPresionAtm IS NOT NULL AND EstadoPresionAtm !='FUSER'),1,0)+
                                       IF((EstadoVelViento IS NOT NULL AND EstadoVelViento !='FUSER'),1,0)+
                                       IF((EstadoPluvio IS NOT NULL AND EstadoPluvio !='FUSER'),1,0)+
                                       IF((EstadoNivel IS NOT NULL AND EstadoNivel !='FUSER'),1,0)+
                                       IF((EstBateria IS NOT NULL AND EstBateria  ='OK'),1,0)
                                       )/
                                       (
                                       IF((EstadoTempAire2m IS NULL),0, 1)+
                                       IF((EstadoTempAire10cm IS NULL),0,1)+
                                       IF((EstadTempS20 IS NULL),0,1)+
                                       IF((EstadoTempS50 IS NULL),0,1)+
                                       IF((EstadoTempS100 IS NULL),0,1)+
                                       IF((EstadoHumAire2m IS NULL),0,1)+
                                       IF((EstadoHumAire10cm IS NULL),0,1)+
                                       IF((EstadoHumaS20 IS NULL),0,1)+
                                       IF((EstadoHumaS30 IS NULL),0,1)+
                                       IF((EstadoHumaS100 IS NULL),0,1)+
                                       IF((EstadoRadicionG IS NULL),0,1)+
                                       IF((EstadoRadiacionV IS NULL),0,1)+
                                       IF((EstadoEvaporacion IS NULL),0,1)+
                                       IF((EstadoPresionAtm IS NULL),0,1)+
                                       IF((EstadoVelViento IS NULL),0,1)+
                                       IF((EstadoPluvio IS NULL),0,1)+
                                       IF((EstadoNivel IS NULL),0,1)+
                                       IF((EstBateria IS NULL OR EstBateria ='DESCONOCIDO'),0,1)+0.01
                                       )
                                   ),
                                  FOREIGN KEY (CODIGO_CAT)
                                  REFERENCES estaciones(CODIGO_CAT)
                                  ON DELETE CASCADE
                                  )"""
  mySQLCursor.execute(q2)

  cnx.close()
except mysql.connector.Error as err:
  if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
    print("Something is wrong with your user name or password")
  elif err.errno == errorcode.ER_BAD_DB_ERROR:
    print("Database does not exist")
  else:
    print(err)

# ARCHIVO QUE RELACION ID_STZ (SCADA) CON ID DE CATALOG IDEMA



pathPrin = 'C:/Python/Python36/RevEstacionesV0.1/CATALOGO+CODSCADA.xlsx'
df = pd.read_excel(pathPrin)
df.head()
df['FECHA_REVISION']=pd.to_datetime(df.FECHA_REVISION,infer_datetime_format=True)
df['FECHA_INST']=pd.to_datetime(df.FECHA_INST,infer_datetime_format=True)
df['FECHA_SUSP']=pd.to_datetime(df.FECHA_SUSP,infer_datetime_format=True)
df['LAST_DATE']=pd.to_datetime(df.LAST_DATE,infer_datetime_format=True)
df[['FECHA_REVISION','FECHA_INST','FECHA_SUSP']].head()

pymysql.converters.encoders[np.float64] = pymysql.converters.escape_float
pymysql.converters.conversions = pymysql.converters.encoders.copy()
pymysql.converters.conversions.update(pymysql.converters.decoders)

dfEstaciones=df[['CODIGO_CAT','AREA_OPERATIVA','NOMBRE','TIPO','CLASE','CATEG','ESTADO_CAT','CORRIENTE','DEPTO','MPIO','LATITUD','LONGITUD','ALTITUD','FECHA_INST','FECHA_SUSP','ID_STZ_SCADA','DCP_ADDRESS','ESTADO2','TRANSMISION','SINIESTRO','VECINO','D_VECINO','VECINO_CORREINTE','D_VECINO_CORRIENTE']]
engine=sqlalchemy.create_engine('mysql+pymysql://root:ideam@localhost:3306/automatizacion')
dfEstaciones.to_sql(name='estaciones',con=engine,index=False,if_exists='append')#popular tabla estaciones
