#############################################################################################################
#                                  @AUTOR: CESAR ALMÉCIGA                      	                        	#
#                                  @PROCESO: FUNCIONES SQL                  					            #                                                     									                        #
#                                  @DESCRIPCIÓN: FUNCIONES BÁSICAS SQL PARA OPTIMIZACIÓN                    #
#                                  Y REUTILIZACIÓN DE CÓDIGO                                                #
#############################################################################################################

# LIBRERIAS E IMPORTES
from sqlalchemy import create_engine
import mysql.connector
from urllib.parse import quote
from sqlalchemy.dialects.mysql import insert
import pandas as pd




## FUNCIÓN DE CONEXIÓN 
## PARAMS: IP: IP SERVIDOR DE BASES DE DATOS
##         PORT: PUERTO SERVIDOR DE BASES DE DATOS
##         USER: USUARIO SERVIDOR DE BASES DE DATOS
##         PASSWORD: CONTRASEÑA SERVIDOR DE BASES DE DATOS
##         BBDD: BBDD SERVIDOR DE BASES DE DATOS
def sql_connection(ip, port, user, password, bbdd):
    sql = create_engine('mysql+pymysql://' + user +':' + quote(password) + '@' + ip + ':' + port + '/' + bbdd, pool_recycle=9600, isolation_level = "AUTOCOMMIT")
    return sql

## FUNCIÓN DE INSERCIÓN
## PARAMS: DATAFRAME: INFORMACIÓN A INSERTAR 
##         TABLENAME: NOMBRE DE TABLA DONDE SE INSERTARÁ LA INFORMACIÓN
##         CONNECTION: VARIABLE DEPENDIENTE DE LA LIBRERIA SQLALCHEMY LA CUAL PERMITE LA 
##         COMUNICACIÓN CON EL SERVIDOR DE BASES DE DATOS
##         TYPE: APPEND/REPLACE --- FORMA EN LA QUE SE INSERTARÁ LA INFORMACIÓN
##         INDEX: ADMITIR INDICES (TRUE/FALSE)
##         CHUNKSIZE: TAMAÑO DE BLOQUES DE INSERCIÓN
def to_sql(dataframe, tableName, connection, type, index, chunksize):
    dataframe.to_sql(name=tableName, con=connection, if_exists = type, index=index, chunksize=chunksize)

## FUNCIÓN DE LECTURA
## PARAMS: SQL: QUERY A EJECUTAR
##         CONNECTION: VARIABLE DEPENDIENTE DE LA LIBRERIA SQLALCHEMY LA CUAL PERMITE LA 
##         COMUNICACIÓN CON EL SERVIDOR DE BASES DE DATOS
def queryRead(sql, connection):
    dataframe = pd.read_sql(sql, connection)
    return dataframe
    
## FUNCIÓN DE EJECUCIÓN
## FUNCIÓN DE LECTURA
## PARAMS: SQL: QUERY A EJECUTAR
##         CONNECTION: VARIABLE DEPENDIENTE DE LA LIBRERIA SQLALCHEMY LA CUAL PERMITE LA 
##         COMUNICACIÓN CON EL SERVIDOR DE BASES DE DATOS
def executeQuery(sql, connection):
    connection.execute(sql)
    