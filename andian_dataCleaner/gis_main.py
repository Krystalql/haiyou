import datetime

import numpy as np
import pandas as pd
import pymysql
from tqdm import tqdm
from logger import LOGGER
from ymlHandler import YamlHandler
from mysql import Mysql
from mysqlconfig import MysqlConfigGis
from commonTools import sqlToDataframe, insertNewToTable
import argparse
import time
from tripleSigmaClean import TripleSigmaCleaner


def getNewData(id: int,
               oldInfo: pd.DataFrame,
               mysqlConfig: MysqlConfigGis,
               validator=None):

    # pd data
    sql = mysqlConfig.srcSelectPdNewById()
    dataPd = sqlToDataframe(mysqlConfig, sql, id)
    if dataPd is None:
        LOGGER.warning("# dataPd no new data.")
        LOGGER.warning(f"# sql: {sql}")
        LOGGER.warning(f"# equip id: {id}")
        return None
    else:
        dataPd = dataPd.fillna(0)

    # sf data
    sql = mysqlConfig.srcSelectSfNewById()
    dataSf = sqlToDataframe(mysqlConfig, sql, id)
    if dataSf is None:
        LOGGER.warning("# dataSf no new data.")
        LOGGER.warning(f"# sql: {sql}")
        LOGGER.warning(f"# equip id: {id}")
        return None
    else:
        dataSf = dataSf.fillna(0)

    # score data
    sql = mysqlConfig.srcSelectScoreNewById()
    dataScore = sqlToDataframe(mysqlConfig, sql, id)
    if dataScore is None:
        LOGGER.warning("# dataScore no new data.")
        LOGGER.warning(f"# sql: {sql}")
        LOGGER.warning(f"# equip id: {id}")
        return None
    else:
        dataScore = dataScore.fillna(0)

    # blob 数组处理
    pdPeak = None
    pdBlob = dataPd['periodic_pulse_signal'].values[0]
    pdArray = np.frombuffer(pdBlob, dtype=np.float32)
    if len(pdArray) == 0:
        # return None
        pdPeak = 0
    else:
        pdPeak = np.max(pdArray)

    # 如果时间契合 进行数据拼接
    dataNew = pd.concat([
        pd.DataFrame({'pd_peak': pdPeak}, index=[0]),
        dataPd.loc[:, 'discharge_amplitude':'pd_type'],
        dataSf.loc[:, 'gis_pressure':'gis_temperature'],
    ],
                        axis=1)
    if validator is not None:
        dataTempForVal = dataNew.drop(columns=['pd_type', 'pd_phase'])
        dataTempForVal = validator.validationNew(dataTempForVal)
        dataTempForVal.insert(3, 'pd_type', dataNew['pd_type'])
        dataTempForVal.insert(2, 'pd_phase', dataNew['pd_phase'])
        dataNew = dataTempForVal

    dataNew = pd.concat([
        dataNew, dataScore.loc[:, 'COMPREHENSIVE_SCORE'],
        dataScore.loc[:, 'RISK_LEVEL']
    ],
                        axis=1)
    dataNew.insert(0, 'input_time', datetime.datetime.now())
    dataNew.insert(
        0, 'equip_name', oldInfo.loc[oldInfo['equip_id'] == id,
                                     'equip_name'].values[0])
    dataNew.insert(0, 'equip_id', id)
    if dataNew.isnull().values.any():
        dataNew = dataNew.fillna(0)
        LOGGER.warning("missing value exists in dataNew.")
    dataNew.insert(0, 'id', None)
    dataNew['input_time'] = dataNew['input_time'].astype(str)

    # 写入数据
    insertNewToTable(mysqlConfig,
                     mysqlConfig.destInsertNew(dataNew.values[0]),
                     tuple(dataNew.values.tolist())[0],
                     many=False)


def gisMain():
    mysqlConfig = MysqlConfigGis()

    # 取出目前表中每台设备时间最新的数据
    # 取出目前表中每台设备时间最新的数据
    # conn = Mysql(host=mysqlConfig.destHOST,
    #              user=mysqlConfig.destUSER,
    #              pwd=mysqlConfig.destPASSWORD,
    #              dbname=mysqlConfig.destDATABASE,
    #              port=mysqlConfig.destPORT)
    conn = pymysql.connect(host=mysqlConfig.destHOST,
                           user=mysqlConfig.destUSER,
                           passwd=mysqlConfig.destPASSWORD,
                           database=mysqlConfig.destDATABASE,
                           port=mysqlConfig.destPORT)
    # sql = mysqlConfig.destSelectOld
    sql = mysqlConfig.srcSelectEquipSectorInfo()
    cur = conn.cursor()
    cur.execute(sql)
    gis_equip_info = cur.fetchall()
    des = cur.description
    header = [[item[0] for item in des]]
    # gis_equip_info, _, header = conn.queryOperation(sql)
    gis_equip_info = pd.DataFrame(np.array(gis_equip_info))
    gis_equip_info.columns = np.array(header)[0]

    # 对以往设备id进行整理 info = （id, name, time）
    gis_equip_sector_info = gis_equip_info.iloc[:, :4]
    idList = set(gis_equip_info['equip_id'].values.tolist())
    # idList = gis_equip_info['equip_id'].values.tolist()
    
    for id in tqdm(idList):
        LOGGER.info(f"Processing equipment: {id}")
        # 初始化3-sigma模块
        # dataOld = gis_equip_info.loc[gis_equip_info['equip_id'] == id]
        # validator = TripleSigmaCleaner(
        #     dataOld.loc[:, 'pd_peak':'gis_temperature'])
        validator = None
        getNewData(id, gis_equip_sector_info, mysqlConfig, validator)

    LOGGER.info(f"Gis data cleaned. Time: {datetime.datetime.now()}")


def parse_arg():
    parse = argparse.ArgumentParser(description='andian datacleaner gis args')
    parse.add_argument("-t",
                       "--time",
                       type=int,
                       default=0,
                       help="time setting(min)")
    parse.add_argument("-i",
                       "--host",
                       type=str,
                       default='47.108.220.99',
                       help="host ip config")
    parse.add_argument("-d",
                       "--database",
                       type=str,
                       default='dqjc_zj_business',
                       help="name of database")
    parse.add_argument("-u",
                       "--user",
                       type=str,
                       default='dqjc_swpu',
                       help="user name")
    parse.add_argument("-w",
                       "--password",
                       type=str,
                       default='swpu@123456',
                       help="password")
    parse.add_argument("-p", "--port", type=int, default=23548, help="port")
    arg = parse.parse_args()

    # write ip INFO into ip.yml
    ymlF = YamlHandler("ip.yml")
    ymlF.write_yaml_data("HOST", arg.host)
    ymlF.write_yaml_data("DATABASE", arg.database)
    ymlF.write_yaml_data("USER", arg.user)
    ymlF.write_yaml_data("PASSWORD", arg.password)
    ymlF.write_yaml_data("PORT", arg.port)
    LOGGER.info("ip config from docker compose has been written into ip.yml")
    LOGGER.info("###### ip info as below:")
    LOGGER.info(f"## HOST: {arg.host}")
    LOGGER.info(f"## DATABSE: {arg.database}:")
    LOGGER.info(f"## USER: {arg.user}")
    LOGGER.info(f"## PASSWORD: {arg.password}")
    LOGGER.info("###### ip info")

    return arg


if __name__ == '__main__':
    args = parse_arg()
    while True:
        gisMain()
        time.sleep(args.time * 60)
        print("一次数据清洗完成")
