import pymysql
import datetime
# from mysqlconfig import MysqlConfigTrans
from tqdm import tqdm
from logger import LOGGER
from ymlHandler import YamlHandler
from mysqlconfig import MysqlConfigTrans
from mysql import Mysql
import pandas as pd
import numpy as np
import datetime
import argparse
import time
from tripleSigmaClean import TripleSigmaCleaner
from gis_main import gisMain
from subcable_main import subcableMain
from commonTools import sqlToDataframe, insertNewToTable


def getNewData(id: int,
               oldInfo: pd.DataFrame,
               mysqlConfig: MysqlConfigTrans,
               validator=None):
    # monitor data
    sql = mysqlConfig.srcSelectMonitorNewById()
    dataMonitor = sqlToDataframe(mysqlConfig, sql, id)
    if dataMonitor is None:
        return None
    else:
        dataMonitor = dataMonitor.fillna(0)

    # grounding current data
    sql = mysqlConfig.srcSelectCurrentNewById()
    dataCurrent = sqlToDataframe(mysqlConfig, sql, id)
    if dataCurrent is None:
        LOGGER.warning("# dataCurrent no new data.")
        LOGGER.warning(f"# sql: {sql}")
        LOGGER.warning(f"# equip id: {id}")
        return None
    else:
        dataCurrent = dataCurrent.fillna(0)

    # oil data
    sql = mysqlConfig.srcSelectOilNewById()
    dataOil = sqlToDataframe(mysqlConfig, sql, id)
    if dataOil is None:
        LOGGER.warning("# dataOil no new data.")
        LOGGER.warning(f"# sql: {sql}")
        LOGGER.warning(f"# equip id: {id}")
        return None
    else:
        dataOil = dataOil.fillna(0)

    # temperature data
    sql = mysqlConfig.srcSelectTemperatureNewById()
    dataTemperature = sqlToDataframe(mysqlConfig, sql, id)
    if dataTemperature is None:
        LOGGER.warning("# dataTemperature no new data.")
        LOGGER.warning(f"# sql: {sql}")
        LOGGER.warning(f"# equip id: {id}")
        return None
    else:
        dataTemperature = dataTemperature.fillna(0)

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

    # 如果时间契合 进行数据拼接
    apparentPower_sqrt = np.square(
        dataMonitor['active_power'].values[0]) + np.square(
            dataMonitor['reactive_power'].values[0])
    if apparentPower_sqrt <= 0:
        LOGGER.warning(
            "Negtive values exist during np.sqrt()('apparentPower') calculation, dealt with 1 replacement."
        )
        apparentPower_sqrt = 1
    apparentPower = np.sqrt(apparentPower_sqrt)
    dataNew = pd.concat([
        dataMonitor.loc[:, 'voltage_LVS_A':'power_factor'],
        dataCurrent.loc[:, 'core_ground_current_data'],
        dataTemperature.loc[:, 'winding_temperature'],
        pd.DataFrame({'apparent_power': apparentPower},
                     index=[0]), dataOil.loc[:, 'H2_ppm':'micro_water_ppm']
    ],
                        axis=1)
    powerFactor = dataMonitor['active_power'].values[0] / apparentPower
    if powerFactor <= 0:
        LOGGER.warning(
            "Negtive values exist during 'powerFactor' calculation, dealt with 1 replacement."
        )
        powerFactor = 1
    dataNew['power_factor'] = powerFactor
    if validator is not None:
        dataNew = validator.validationNew(dataNew)
    dataNew = pd.concat([
        dataNew, dataScore.loc[:, 'COMPREHENSIVE_SCORE'],
        dataScore.loc[:, 'RISK_LEVEL']
    ],
                        axis=1)
    dataNew.insert(0, 'acquisition_time', datetime.datetime.now())
    dataNew.insert(
        0, 'equip_name', oldInfo.loc[oldInfo['equip_id'] == id,
                                     'equip_name'].values[0])
    dataNew.insert(0, 'equip_id', id)
    if dataNew.isnull().values.any():
        dataNew = dataNew.fillna(0)
        LOGGER.warning("missing value exists in dataNew.")
    dataNew.insert(0, 'id', None)
    dataNew['acquisition_time'] = dataNew['acquisition_time'].astype(str)

    # 写入数据
    insertNewToTable(mysqlConfig,
                     mysqlConfig.destInsertNew(dataNew.values[0]),
                     tuple(dataNew.values.tolist())[0],
                     many=False)

    # 进行average数据的拼接和insert
    dataAverage = pd.concat([
        pd.DataFrame(
            {
                'voltage_average':
                np.average(dataMonitor.loc[:, 'voltage_HVS_A':'voltage_HVS_C'].
                           values.astype('float')),
                'voltage_max':
                np.max(dataMonitor.loc[:, 'voltage_HVS_A':'voltage_HVS_C'].
                       values.astype('float')),
                'current_average':
                np.average(dataMonitor.loc[:, 'current_HVS_A':'current_HVS_C'].
                           values.astype('float')),
                'current_max':
                np.max(dataMonitor.loc[:, 'current_HVS_A':'current_HVS_C'].
                       values.astype('float')),
            },
            index=[0]), dataNew.loc[:, 'power_factor'],
        dataCurrent.loc[:, 'core_ground_current_data'],
        dataTemperature.loc[:, 'winding_temperature'],
        dataMonitor.loc[:, 'active_power':'reactive_power'],
        pd.DataFrame({'apparent_power': 0},
                     index=[0]), dataOil.loc[:, 'H2_ppm':'micro_water_ppm'],
        dataScore.loc[:, 'COMPREHENSIVE_SCORE'], dataScore.loc[:, 'RISK_LEVEL']
    ],
                            axis=1)
    dataAverage.insert(0, 'acquisition_time', datetime.datetime.now())
    dataAverage.insert(
        0, 'equip_name', oldInfo.loc[oldInfo['equip_id'] == id,
                                     'equip_name'].values[0])
    dataAverage.insert(0, 'equip_id', id)
    if dataAverage.isnull().values.any():
        dataAverage = dataAverage.fillna(0)
        LOGGER.warning("missing value exists in dataAverage.")
    dataAverage.insert(0, 'id', None)
    dataAverage['acquisition_time'] = dataAverage['acquisition_time'].astype(
        str)
    insertNewToTable(mysqlConfig,
                     mysqlConfig.destInsertNewAverage(dataAverage.values[0]),
                     tuple(dataAverage.values.tolist())[0],
                     many=False)


def trans_main():
    mysqlConfig = MysqlConfigTrans()

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
    sql = mysqlConfig.srcSelectEquipId()
    cur = conn.cursor()
    cur.execute(sql, 11)
    data_old = cur.fetchall()
    des = cur.description
    header = [[item[0] for item in des]]
    # data_old, _, header = conn.queryOperation(sql)
    data_old = pd.DataFrame(np.array(data_old))
    data_old.columns = np.array(header)[0]

    # 对以往设备id进行整理 info = （id, name, time）
    oldInfo = data_old.iloc[:, :4]
    # idList = set(data_old['equip_id'].values.tolist())
    idList = data_old['equip_id'].values.tolist()
    for id in tqdm(idList):
        LOGGER.info(f"Processing equipment: {id}")
        # 初始化3-sigma模块
        dataOld = data_old.loc[data_old['equip_id'] == id]
        # validator = TripleSigmaCleaner(
        #     dataOld.loc[:, 'voltage_LVS_A':'micro_water_ppm'])
        validator = None
        getNewData(id, oldInfo, mysqlConfig, validator)

    LOGGER.info(f"Transformer data cleaned. Time: {datetime.datetime.now()}")


def parse_arg():
    parse = argparse.ArgumentParser(description='andian cause trans args')
    parse.add_argument("-t",
                       "--time",
                       type=int,
                       default=1,
                       help="time setting(min)")
    parse.add_argument("-i",
                       "--host",
                       type=str,
                       default='127.0.0.1',
                       help="host ip config")
    parse.add_argument("-d",
                       "--database",
                       type=str,
                       default='dqjc_zj_business_4_8',
                       help="name of database")
    parse.add_argument("-u",
                       "--user",
                       type=str,
                       default='root',
                       help="user name")
    parse.add_argument("-w",
                       "--password",
                       type=str,
                       default='123456',
                       help="password")
    parse.add_argument("-p", "--port", type=int, default=3306, help="port")
    arg = parse.parse_args()

    # write ip INFO into ip.yml
    if arg.host:
        ymlF = YamlHandler("ip.yml")
        ymlF.write_yaml_data("HOST", arg.host)
        ymlF.write_yaml_data("DATABASE", arg.database)
        ymlF.write_yaml_data("USER", arg.user)
        ymlF.write_yaml_data("PASSWORD", arg.password)
        ymlF.write_yaml_data("PORT", arg.port)
        LOGGER.info(
            "ip config from docker compose has been written into ip.yml")
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
        LOGGER.info("#" * 100)
        LOGGER.info("### Trans start...")
        trans_main()
        print("变压器数据清洗完成")
        LOGGER.info("### GIS start...")
        gisMain()
        print("GIS数据清洗完成")
        LOGGER.info("### Subcable start...")
        subcableMain()
        print("海缆数据清洗完成")
        time.sleep(args.time * 60)
