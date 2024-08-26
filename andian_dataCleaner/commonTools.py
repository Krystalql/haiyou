import numpy as np
import pandas as pd
import pymysql

from logger import LOGGER


def sqlToDataframe(conn, sql, id):
    """
    @ warn: conn每次都进行新建和销毁
    """
    conn = pymysql.connect(host=conn.srcHOST,
                           user=conn.srcUSER,
                           password=conn.srcPASSWORD,
                           database=conn.srcDATABASE,
                           port=conn.srcPORT)
    conn.ping(reconnect=True)
    cur = conn.cursor()
    try:
        cur.execute(sql, id)
        data = cur.fetchall()
        des = cur.description
        cur.close()
        conn.close()
    except pymysql.Error as e:
        data = None
        print(e)
        LOGGER.error(e)
        conn.rollback()
        cur.close()
        conn.close()

    if len(data) == 0:
        return None
    header = [[item[0] for item in des]]
    data = pd.DataFrame(data)
    data.columns = np.array(header)[0]

    return data


def insertNewToTable(conn, sql, data, many: bool):
    conn = pymysql.connect(host=conn.destHOST,
                           user=conn.destUSER,
                           password=conn.destPASSWORD,
                           database=conn.destDATABASE,
                           port=conn.destPORT)
    cur = conn.cursor()
    conn.ping(reconnect=True)
    try:
        if many:
            cur.executemany(sql, tuple(data))
            conn.commit()
            cur.close()
            conn.close()
        else:
            cur.execute(sql, data)
            conn.commit()
            cur.close()
            conn.close()
    except pymysql.Error as e:
        LOGGER.error(e)
        LOGGER.error(f"Error sql: {sql}")
        conn.rollback()
        cur.close()
        conn.close()
