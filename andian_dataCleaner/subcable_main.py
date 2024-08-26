import argparse
import datetime
import time
import numpy as np
import pandas as pd
import pymysql
from tqdm import tqdm
from logger import LOGGER
from ymlHandler import YamlHandler
from logger import LOGGER
from mysql import Mysql
from mysqlconfig import MysqlConfigSubcable
from commonTools import sqlToDataframe, insertNewToTable
from tripleSigmaClean import TripleSigmaCleaner


def sector_choose(temper_opti, temper_core, disturb):
    def pick_arr(arr, num):
        if num > len(arr):
            print("Return original array")
            return arr
        else:
            output = np.array([], dtype=arr.dtype)
            seg = len(arr) / num
            idx = np.array([], dtype=int)
            for n in range(num):
                if int(seg * (n + 1)) >= len(arr):
                    output = np.append(output, arr[-1])
                    idx = np.append(idx, np.where(arr == arr[-1])[0][-1])
                else:
                    output = np.append(output, arr[int(seg * n)])
                    idx = np.append(idx, int(seg * n))
            return [output, idx]

    temper_opti_arr_ori, temper_core_arr_ori, disturb_arr_ori = np.frombuffer(temper_opti, np.float32), \
                                                                np.frombuffer(temper_core, np.float32), \
                                                                np.frombuffer(disturb, np.float32)
    temper_opti_arr, temper_core_arr, disturb_arr = pick_arr(temper_opti_arr_ori, 50), \
                                                    pick_arr(temper_core_arr_ori, 50), \
                                                    pick_arr(disturb_arr_ori, 50)

    return temper_opti_arr[0], temper_core_arr[0], disturb_arr[0]


def getNewData(id: int,
               oldInfo: pd.DataFrame,
               mysqlConfig: MysqlConfigSubcable,
               validator=None):

    # monitor data
    sql = mysqlConfig.srcSelectMonitorNewById()
    dataMonitor = sqlToDataframe(mysqlConfig, sql, id)
    if dataMonitor is None:
        LOGGER.warning("# dataMonitor no new data.")
        LOGGER.warning(f"# sql: {sql}")
        LOGGER.warning(f"# equip: {id}")
        return None
    else:
        dataMonitor = dataMonitor.fillna(0)

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

    # disturbance data
    sql = mysqlConfig.srcSelectDisturbanceNewById()
    dataDisturbance = sqlToDataframe(mysqlConfig, sql, id)
    if dataDisturbance is None:
        LOGGER.warning("# dataDisturbance no new data.")
        LOGGER.warning(f"# sql: {sql}")
        LOGGER.warning(f"# equip id: {id}")
        return None
    else:
        dataDisturbance = dataDisturbance.fillna(0)

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


    # temperature opti blob处理
    temperOptiBlob, temperCoreBlob, disturbBlob = dataTemperature['TEMPERATURE_OPTI'].values[0], \
                                                  dataTemperature['TEMPERATURE_CABLECORE'].values[0], \
                                                  dataDisturbance['DISTURBANCE_ENERGY'].values[0]
    temperOptiArr, temperCoreArr, disturbArr = sector_choose(
        temperOptiBlob, temperCoreBlob, disturbBlob)

    # 视在功率计算
    apparentPower_sqrt = np.square(
        dataMonitor['ACTIVE_POWER'].values[0]) + np.square(
            dataMonitor['REACTIVE_POWER'].values[0])
    if apparentPower_sqrt <= 0:
        LOGGER.warning(
            "Negtive values exist during np.sqrt() calculation, dealt with 1 replacement."
        )
        LOGGER.warning(
            f"active_power: {dataMonitor['ACTIVE_POWER'].values[0]}, reactive_power: {dataMonitor['REACTIVE_POWER'].values[0]}, sq: {apparentPower_sqrt}"
        )
        apparentPower_sqrt = 1
    apparentPower = np.sqrt(apparentPower_sqrt)
    powerFactor = float(dataMonitor['ACTIVE_POWER'].values[0]) / float(apparentPower)
    if powerFactor <= 0:
        LOGGER.warning(
            "Negtive values exist during 'powerFactor' calculation, dealt with 1 replacement."
        )
        powerFactor = 1
    dataMonitor['POWER_FACTOR'] = powerFactor
    dataMonitor.insert(dataMonitor.shape[-1], 'apparent_power', apparentPower)

    # 如果时间契合 进行数据拼接
    dataCombined = []  # final shape: [10, num_feature]
    i = 0
    for zipped in zip(temperOptiArr, temperCoreArr, disturbArr):
        i += 1
        temp = dataMonitor.loc[:, 'VOLTAGE_PHASE_A':'apparent_power']
        temp_zipped = pd.DataFrame(zipped).T
        temp_zipped.columns = [
            'TEMPERATURE_OPTI', 'TEMPERATURE_CABLECORE', 'DISTURBANCE_ENERGY'
        ]
        temp = pd.concat([temp, temp_zipped], axis=1)
        temp.insert(0, 'monitor_id', id + f'_{i}')
        try:
            dataCombined = pd.concat([dataCombined, temp], axis=0)
        except:
            dataCombined = temp

    if validator is not None:
        # dataNew = dataCombined.copy(True)
        dataNew = dataCombined.drop(columns=['monitor_id'])
        dataNew = validator.validationNew(dataNew)
        dataCombined = dataCombined.reset_index(drop=True)
        dataNew.insert(0, 'monitor_id', pd.Series(dataCombined['monitor_id']))
    else:
        dataCombined = dataCombined.reset_index(drop=True)
        dataNew = dataCombined

    dataNew = pd.concat([
        dataNew,
        pd.DataFrame([dataScore.loc[:, 'COMPREHENSIVE_SCORE'].values[0]] * 50),
        pd.DataFrame([dataScore.loc[:, 'RISK_LEVEL'].values[0]] * 50)
    ],
                        axis=1)
    dataNew.insert(1, 'input_time', datetime.datetime.now())
    dataNew.insert(
        0, 'equip_name', oldInfo.loc[oldInfo['equip_id'] == id,
                                     'equip_name'].values[0])
    dataNew.insert(0, 'equip_id', id)
    if dataNew.isnull().values.any():
        dataNew = dataNew.fillna(0)
        LOGGER.warning("missing value exists in dataNew.")
    dataNew.insert(0, 'id', None)
    dataNew['input_time'] = dataNew['input_time'].astype(str)

    # average 数据
    dataAverage = pd.concat([
        pd.DataFrame(
            {
                'voltage_average':
                    np.average(dataNew.loc[:, 'VOLTAGE_PHASE_A':'VOLTAGE_PHASE_C'].
                               values.astype('float'), axis=1),
                'voltage_max':
                    np.max(dataNew.loc[:, 'VOLTAGE_PHASE_A':'VOLTAGE_PHASE_C'].
                           values.astype('float'), axis=1),
                'current_average':
                    np.average(dataNew.loc[:, 'CURRENT_PHASE_A':'CURRENT_PHASE_C'].
                               values.astype('float'), axis=1),
                'current_max':
                    np.max(dataNew.loc[:, 'CURRENT_PHASE_A':'CURRENT_PHASE_C'].
                           values.astype('float'), axis=1),
            },
            index=[i for i in range(len(dataNew))]),
        dataNew.loc[:, 'POWER_FACTOR'],
        dataNew.loc[:, 'apparent_power'],
        dataNew.loc[:, 'TEMPERATURE_OPTI'],
        dataNew.loc[:, 'TEMPERATURE_CABLECORE'],
        dataNew.loc[:, 'DISTURBANCE_ENERGY'],
        dataNew.iloc[:, -2], dataNew.iloc[:, -1]
    ], axis=1)
    dataAverage.insert(0, 'acquisition_time', datetime.datetime.now())
    dataAverage.insert(0, 'monitor_id', dataCombined.loc[:, 'monitor_id'])
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

    # 写入数据
    insertNewToTable(mysqlConfig,
                     mysqlConfig.destInsertNew(dataNew.values[0]),
                     tuple(dataNew.values.tolist()),
                     many=True)
    insertNewToTable(mysqlConfig,
                     mysqlConfig.destInsertNewAverage(dataAverage.values[0]),
                     tuple(dataAverage.values.tolist()),
                     many=True)


def subcableMain():
    mysqlConfig = MysqlConfigSubcable()

    # 取出目前表中每台设备时间最新的数据
    # conn = Mysql(host=mysqlConfig.destHOST,
    #              user=mysqlConfig.destUSER,
    #              pwd=mysqlConfig.destPASSWORD,
    #              dbname=mysqlConfig.destDATABASE,
    #              port=mysqlConfig.destPORT)
    # sql = mysqlConfig.destSelectOld
    # data_old, _, header = conn.queryOperation(sql)
    # data_old = pd.DataFrame(np.array(data_old))
    # data_old.columns = np.array(header)[0]

    conn = pymysql.connect(host=mysqlConfig.destHOST,
                           user=mysqlConfig.destUSER,
                           passwd=mysqlConfig.destPASSWORD,
                           database=mysqlConfig.destDATABASE,
                           port=mysqlConfig.destPORT)
    # sql = mysqlConfig.destSelectOld
    sql = mysqlConfig.srcSelectEquipId()
    cur = conn.cursor()
    cur.execute(sql, 1)
    data_old = cur.fetchall()
    des = cur.description
    header = [[item[0] for item in des]]
    # data_old, _, header = conn.queryOperation(sql)
    data_old = pd.DataFrame(np.array(data_old))
    data_old.columns = np.array(header)[0]

    # 对以往设备id进行整理 info = （id, name, monitorId, time）
    oldInfo = data_old.iloc[:, 0:5]
    idList = set(data_old['equip_id'].values.tolist())
    for id in tqdm(idList):
        LOGGER.info(f"Processing equipment: {id}")
        # 初始化3-sigma模块
        dataOld = data_old.loc[data_old['equip_id'] == id]
        # validator = TripleSigmaCleaner(
        #     dataOld.loc[:, 'voltage_phase_A':'disturbance_energy'])
        validator = None
        getNewData(id, oldInfo, mysqlConfig, validator)

    LOGGER.info(f"Subcable data cleaned. Time: {datetime.datetime.now()}")


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
                       default='127.0.0.1',
                       help="host ip config")
    parse.add_argument("-d",
                       "--database",
                       type=str,
                       default='dqjc_zj_business',
                       help="name of database")
    parse.add_argument("-u",
                       "--user",
                       type=str,
                       default='root',
                       help="user name")
    parse.add_argument("-w",
                       "--password",
                       type=str,
                       default='wangziycy',
                       help="password")
    parse.add_argument("-p", "--port", type=int, default=3306, help="port")
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
        subcableMain()
        time.sleep(args.time * 60)
        print("海缆数据清洗完成")
