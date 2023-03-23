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

path_asignaciones = r"C:\MOVISTAR_COLOMBIA\02_CRUDOS\ASIGNACION\PROVISION"
fileDir(path_asignaciones)


files = []
read_asg = []
df_prov = pd.DataFrame()


i = 0
# read_asg = [pd.ExcelFile(name) for name in glob.glob("churn*.xlsx")]
for filename in glob.glob("*FJ*.csv"):
    read_txt = pd.read_csv(filename, encoding='latin1', sep=';', quotechar='"')
    df_prov = pd.concat([read_txt, df_prov], axis=0)
    files.append(filename)

# df_prov = [x.parse(index_col=None) for x in read_asg]


for x in range(len(files)):
    sql = """DELETE FROM `bbdd_cs_bog_movistar_ciclos`.`tb_asignacion_provision_fija_movistar_colombia`
            WHERE TIPO_BASE = '""" + files[x] + """';"""
    connection.execute(sql)


df_prov['TIPO_BASE'] = files[i]
df_prov['FECHA_ARCHIVO'] = (re.findall("[0-9][0-9]+",files[i])[0])
df_prov['FECHA_ASIGNACION'] = (re.findall("[0-9][0-9]+",files[i])[0])
df_prov['FECHA_ARCHIVO'] = pd.to_datetime(df_prov['FECHA_ARCHIVO'], format="%Y%m%d")
df_prov['FECHA_ASIGNACION'] = pd.to_datetime(df_prov['FECHA_ASIGNACION'], format="%Y%m%d")
df_prov['FECHA_CIERRE'] = pd.to_datetime(df_prov['FECHA_CIERRE'], format="%d/%m/%Y")


to_sql(df_prov, "tb_asignacion_provision_fija_movistar_colombia", connection,'append',None,10000)

connection.close()


