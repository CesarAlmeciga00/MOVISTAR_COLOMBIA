import sys
import re
sys.path.append("C:/MOVISTAR_COLOMBIA/01_SCRIPTS/py")
from importsAndFunctions import *



ip = "172.17.8.68"
port = "3306"
user = "cesaralmeciga5850"
password = "U@SXQFNiEeShz66wxgzt"
bbdd = "bbdd_cs_bog_movistar_ciclos"
sqlEngine = sql_connection(ip, port, user, password, bbdd)
connection = sqlEngine.connect()

path_asignaciones = r"C:\MOVISTAR_COLOMBIA\02_CRUDOS\ASIGNACION\CHURN"
fileDir(path_asignaciones)
files = []
read_asg = []
frames_asg = []


i = 0
# read_asg = [pd.ExcelFile(name) for name in glob.glob("churn*.xlsx")]
for filename in glob.glob("*churn*.xlsx"):
    read_asg.append(pd.ExcelFile(filename))
    files.append(filename)

# frames_asg = [x.parse(index_col=None) for x in read_asg]
for x in read_asg:
    frames_asg.append(x.parse(index_col=None))
    frames_asg[i]['TIPO_BASE'] = files[i]
    frames_asg[i]['FECHA_ARCHIVO'] = (re.findall("[0-9][0-9]+",files[i])[0])
    frames_asg[i]['FECHA_ASG'] = (re.findall("[0-9][0-9]+",files[i])[0])
    frames_asg[i]['FECHA_CIERRE'] = (re.findall("[0-9][0-9]+",files[i])[0])
    i+=1
df_churn = pd.concat(frames_asg)


for x in range(len(files)):
    sql = """DELETE FROM `bbdd_cs_bog_movistar_ciclos`.`tb_asignacion_churn_fija_movistar_colombia`
            WHERE TIPO_BASE = '""" + files[x] + """';"""
    connection.execute(sql)


df_churn['FEC_ESTADO'] = pd.to_datetime(df_churn['FEC_ESTADO'], format="%d/%m/%Y")
df_churn['FEC_ALTA'] = pd.to_datetime(df_churn['FEC_ALTA'], format="%d/%m/%Y")
df_churn['FEC_ESTADO_CRM'] = pd.to_datetime(df_churn['FEC_ESTADO_CRM'], format="%d/%m/%Y")
df_churn['FECHA_ARCHIVO'] = pd.to_datetime(df_churn['FECHA_ARCHIVO'], format="%y%m%d")
df_churn['FECHA_ASG'] = pd.to_datetime(df_churn['FECHA_ASG'], format="%y%m%d")
df_churn['FECHA_CIERRE'] = pd.to_datetime(df_churn['FECHA_CIERRE'], format="%y%m%d")
# df_churn['FECHA_ASG'] = df_churn.FECHA_ASG + pd.offsets.MonthBegin(1)
df_churn['FECHA_CIERRE'] = df_churn.FECHA_CIERRE + pd.offsets.MonthEnd(2)
to_sql(df_churn, "tb_asignacion_churn_fija_movistar_colombia", connection,'append',None,10000)

# sql = """INSERT IGNORE INTO `bbdd_cs_bog_movistar_mexico`.`tb_reporte_casos_b2b`
#         SELECT *
#         FROM `bbdd_cs_bog_movistar_mexico`.`tb_asignacion_churn_fija_movistar_colombia`;"""

# connection.execute(sql)

# sql = """DROP TABLE `bbdd_cs_bog_movistar_mexico`.`tb_asignacion_churn_fija_movistar_colombia`;"""
        
# connection.execute(sql)

connection.close()