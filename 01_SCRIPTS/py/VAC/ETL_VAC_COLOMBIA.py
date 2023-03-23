import sys
sys.path.append("C:/MOVISTAR_COLOMBIA/01_SCRIPTS/py")
from importsAndFunctions import *



ip = "172.17.8.68"
port = "3306"
user = "cesaralmeciga5850"
password = "U@SXQFNiEeShz66wxgzt"
bbdd = "bbdd_cs_bog_movistar_ciclos"
sqlEngine = sql_connection(ip, port, user, password, bbdd)
connection = sqlEngine.connect()

path_asignaciones = r"C:\MOVISTAR_COLOMBIA\02_CRUDOS\ASIGNACION\VAC"
fileDir(path_asignaciones)
df_vac = pd.DataFrame()
for filename in glob.glob("*VC*.txt"):
    read_txt = pd.read_csv(filename, encoding='latin1', sep='|', on_bad_lines='skip', skipfooter=1, dtype={'NUM_FINANCIACION         ':'string', 'NUM_ABONADO_SCL':'string', 'COD_CUENTA_DAVOX         ':'string'})
    df_vac = pd.concat([read_txt, df_vac], axis=0)

df_vac = df_vac.rename(columns=lambda x: x.strip())
cols = df_vac.columns
df_vac[cols] = df_vac[cols].astype(str).apply(lambda x: x.str.strip())

df_vac['FECHA_ASIGNACION'] = pd.to_datetime(df_vac['FECHA_ASIGNACION'], format="%d/%m/%Y")
df_vac['FECHA_CIERRE'] = pd.to_datetime(df_vac['FECHA_CIERRE'], format="%d/%m/%Y")
df_vac['FEC_VENTA_SCL'] = pd.to_datetime(df_vac['FEC_VENTA_SCL'], format="%d/%m/%Y")
df_vac['FEC_CARGA_DAVOX'] = pd.to_datetime(df_vac['FEC_CARGA_DAVOX'], format="%d/%m/%Y")

df_delete = df_vac['FECHA_CIERRE']
df_delete = df_delete.drop_duplicates()


sql = "DELETE FROM tb_asignacion_vac_movistar_colombia WHERE FECHA_CIERRE = '" + str(df_delete[0]) + "'"

connection.execute(sql)

to_sql(df_vac, "tb_asignacion_vac_movistar_colombia", connection,'append',None,10000)

# sql = """DROP TABLE `bbdd_cs_bog_movistar_mexico`.`tb_asignacion_churn_fija_movistar_colombia`;"""
        
# connection.execute(sql)

connection.close()