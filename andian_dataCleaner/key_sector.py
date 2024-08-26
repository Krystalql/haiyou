import pymysql
from mysqlconfig import MysqlConfigSubcable

mysqlConfig = MysqlConfigSubcable()
conn = pymysql.connect(host=mysqlConfig.destHOST,
                       user=mysqlConfig.destUSER,
                       password=mysqlConfig.destPASSWORD,
                       database=mysqlConfig.destDATABASE,
                       port=mysqlConfig.destPORT)
cur = conn.curcor()

for i in range(50):
    monitorIndex = i + 101
    listToW = [].append(None)
    sql = f"INSERT INTO test_subcable_key_sector VALUES({})"
    