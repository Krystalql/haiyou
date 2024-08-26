import argparse
import time
import numpy as np
import pandas as pd
import pymysql
import shap
from torch.autograd import Variable
from logger import LOGGER
from ymlHandler import YamlHandler
from mysqlconfig import MysqlConfig
from dataProcess import *
from mmd import *
from tqdm import tqdm
from mysql import Mysql
from sklearn.ensemble import RandomForestClassifier as XGBClassifier
import faulthandler

faulthandler.enable()

pymysql.converters.encoders[np.ndarray] = pymysql.converters.escape_float
pymysql.converters.conversions = pymysql.converters.encoders.copy()
pymysql.converters.conversions.update(pymysql.converters.decoders)

np.seterr(divide='ignore', invalid='ignore')

mysqlConfig = MysqlConfig()


def softmax(x, axis=1):
    # 计算每行的最大值
    row_max = x.max(axis=axis)

    # 每行元素都需要减去对应的最大值，否则求exp(x)会溢出，导致inf情况
    row_max = row_max.reshape(-1, 1)
    x = x - row_max

    # 计算e的指数次幂
    x_exp = np.exp(x)
    x_sum = np.sum(x_exp, axis=axis, keepdims=True)
    s = x_exp / x_sum
    return s


def select_cable(data, subcable_name):
    chosen_trans = data.loc[data['equip_id'] == subcable_name]
    return chosen_trans.drop('equip_id', axis=1)


def select_cable_sector(data, sector_name):
    chosen_trans = data.loc[data['equip_sector_id'] == sector_name]
    return chosen_trans


def select_cable_monitor(data, monitor_name):
    chosen_trans = data.loc[data['monitor_id'] == monitor_name]
    return chosen_trans.drop('monitor_id', axis=1)


def clear_table(tabel_name):
    conn = pymysql.connect(mysqlConfig.HOST, mysqlConfig.USER,
                           mysqlConfig.PASSWORD, mysqlConfig.DATABASE,
                           mysqlConfig.PORT)
    cursor = conn.cursor()
    for i in tabel_name:
        sql = f"DELETE FROM {i}"
        try:
            cursor.execute(sql)
            conn.commit()
        except pymysql.Error as exc:
            LOGGER.error(exc)
            conn.rollback()
    cursor.close()
    conn.close()


def mmd_compare(data, data_best, features_name, id, name, monitor, id_best,
                name_best, monitor_best):
    data = data.values
    data_best = data_best.values
    mmd_result = []
    monitor = id + '_' + str(monitor)

    for i in range(len(features_name)):
        temp = data[:, i]
        temp = temp.reshape(len(data), 1)
        temp_bet = data_best[:, i]
        temp_bet = temp_bet.reshape(len(data_best), 1)
        temp = temp.astype(float)
        temp_bet = temp_bet.astype(float)

        temp_x = torch.from_numpy(temp)
        temp_y = torch.from_numpy(temp_bet)
        temp_x, temp_y = Variable(temp_x), Variable(temp_y)
        mmd_temp_res = mmd_rbf(temp_x, temp_y)
        mmd_temp_res = float(mmd_temp_res)
        mmd_result.append(mmd_temp_res)
    data = data.astype('float')
    data_better = data_best.astype('float')
    x, y = torch.from_numpy(data), torch.from_numpy(data_better)
    x, y = Variable(x), Variable(y)
    all_mmd = float(mmd_rbf(x, y))
    mmd_result.append(all_mmd)
    mmd_result = np.array(mmd_result).reshape(-1)

    # 取平均值形成层次结构
    # mmd分析中，如果完全一致，rbf核的mmd-loss输出会变成nan，因此先填补向量中的nan，防止后面的计算出错
    mmd_result = pd.DataFrame(mmd_result)
    mmd_result = mmd_result.fillna(0)
    mmd_result = mmd_result.values.reshape(-1)
    vol = np.sum(mmd_result[:3])
    cur = np.sum(mmd_result[3:6])
    power = np.sum(mmd_result[6:9])
    temper = np.sum(mmd_result[9:11])
    disturb = mmd_result[11]
    electric = vol + cur + power

    head = np.array([
        None, f'{id}', f'{name}', f'{monitor}', f'{id_best}', f'{name_best}',
        f'{monitor_best}'
    ])
    rest = np.array(
        [vol, cur, power, temper, disturb, electric, temper, disturb])
    mmd_result = np.concatenate([head, mmd_result, rest])
    mmd_result = pd.DataFrame(mmd_result)
    mmd_result = mmd_result.fillna(0)
    mmd_result = mmd_result.values.reshape(-1)
    mmd_result[0] = None

    write_mmd_2_sql('test_subcable_mmd_good', mmd_result.tolist())


def write_layer_2_sql(table_name, layer_data):
    # 对node_name的每一个值，更换为对应的中文。
    node_names = {
        'RiskLevel': '风险',
        'electric': '电参量',
        'voltage': '电压',
        'voltage_phase_A': '电压-A相',
        'voltage_phase_B': '电压-B相',
        'voltage_phase_C': '电压-C相',
        'current': '电流',
        'current_phase_A': '电流-A相',
        'current_phase_B': '电流-B相',
        'current_phase_C': '电流-C相',
        'power': '功率',
        'active_power': '有功功率',
        'reactive_power': '无功功率',
        'power_factor': '功率因数',
        'temperature': '温度',
        'temperature_opti': '光纤温度',
        'temperature_cablecore': '缆芯温度',
        'disturb': '扰动',
        'disturbance': '扰动',
        'disturbance_energy': '扰动能量'
    }
    layer_data = list(layer_data)
    layer_data[3] = node_names[f'{layer_data[3]}']
    layer_data = tuple(layer_data)

    conn = pymysql.connect(mysqlConfig.HOST, mysqlConfig.USER,
                           mysqlConfig.PASSWORD, mysqlConfig.DATABASE,
                           mysqlConfig.PORT)
    cursor = conn.cursor()
    sql = f"INSERT INTO {table_name}(equip_id,equip_name,monitor_id,node_name,node_factor,parent_id) VALUES  " \
          f"(%s, %s, %s, %s, %s, %s)"
    args = layer_data  # 对应表格的数据

    try:
        cursor.execute(sql, args)  # 执行相关操作
        conn.commit()  # 更新数据库
    except Exception as exc:
        LOGGER.error(exc)
        conn.rollback()
    cursor.close()
    conn.close()


def write_xgb_2_sql(table_name, layer_data):
    conn = pymysql.connect(mysqlConfig.HOST, mysqlConfig.USER,
                           mysqlConfig.PASSWORD, mysqlConfig.DATABASE,
                           mysqlConfig.PORT)
    cursor = conn.cursor()
    sql = f"INSERT INTO {table_name} VALUES  " \
          f"(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    args = layer_data  # 对应表格的数据

    try:
        cursor.execute(sql, args)  # 执行相关操作
        conn.commit()  # 更新数据库
    except pymysql.Error as exc:
        LOGGER.error(exc)
        conn.rollback()
    cursor.close()
    conn.close()


def write_mmd_2_sql(table_name, layer_data):
    conn = pymysql.connect(mysqlConfig.HOST, mysqlConfig.USER,
                           mysqlConfig.PASSWORD, mysqlConfig.DATABASE,
                           mysqlConfig.PORT)
    cursor = conn.cursor()
    sql = f"INSERT INTO {table_name} VALUES  " \
          f"(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    args = layer_data  # 对应表格的数据

    try:
        cursor.execute(sql, args)  # 执行相关操作
        conn.commit()  # 更新数据库
    except pymysql.Error as exc:
        LOGGER.error(exc)
        conn.rollback()
    cursor.close()
    conn.close()


def query_node_id_4_layer(node_name):
    # 对node_name的每一个值，更换为对应的中文。
    node_names = {
        'RiskLevel': '风险',
        'electric': '电参量',
        'voltage': '电压',
        'voltage_phase_A': '电压-A相',
        'voltage_phase_B': '电压-B相',
        'voltage_phase_C': '电压-C相',
        'current': '电流',
        'current_phase_A': '电流-A相',
        'current_phase_B': '电流-B相',
        'current_phase_C': '电流-C相',
        'power': '功率',
        'active_power': '有功功率',
        'reactive_power': '无功功率',
        'power_factor': '功率因数',
        'temperature': '温度',
        'temperature_opti': '光纤温度',
        'temperature_cablecore': '缆芯温度',
        'disturb': '扰动',
        'disturbance': '扰动',
        'disturbance_energy': '扰动能量'
    }
    node_name = node_names[f'{node_name}']

    conn = pymysql.connect(mysqlConfig.HOST, mysqlConfig.USER,
                           mysqlConfig.PASSWORD, mysqlConfig.DATABASE,
                           mysqlConfig.PORT)
    cursor = conn.cursor()
    if node_name == 'last':
        sql = f"SELECT id FROM test_subcable_tree_explain ORDER BY id DESC LIMIT 1"
    else:
        sql = f"SELECT id FROM test_subcable_tree_explain WHERE node_name='{node_name}' ORDER BY id DESC LIMIT 1"
    try:
        cursor.execute(sql)
        init_id = cursor.fetchall()
        conn.commit()
    except Exception as exc:
        LOGGER.error(exc)
        conn.rollback()
    cursor.close()
    conn.close()

    return str(init_id[0][0])


def cengci(shapley):
    layer1 = ['electric', 'temperature', 'disturb']
    layer2 = ['voltage', 'current', 'power', 'temperature', 'disturbance']

    # Layer2.
    vol = shapley.apply(lambda x: x['voltage_phase_A'] + x['voltage_phase_B'] +
                        x['voltage_phase_C'],
                        axis=1)
    cur = shapley.apply(lambda x: x['current_phase_A'] + x['current_phase_B'] +
                        x['current_phase_C'],
                        axis=1)
    pwe = shapley.apply(
        lambda x: x['active_power'] + x['reactive_power'] + x['power_factor'],
        axis=1)
    tep = shapley.apply(
        lambda x: x['temperature_opti'] + x['temperature_cablecore'], axis=1)
    dit = shapley.apply(lambda x: x['disturbance_energy'], axis=1)

    layer2_df = pd.concat([vol, cur, pwe, tep, dit], axis=1)
    layer2_df.columns = layer2

    # Layer1.
    ele = layer2_df.apply(lambda x: x['voltage'] + x['current'] + x['power'],
                          axis=1)
    tep = layer2_df.apply(lambda x: x['temperature'], axis=1)
    dit = layer2_df.apply(lambda x: x['disturbance'], axis=1)

    layer1_df = pd.concat([ele, tep, dit], axis=1)
    layer1_df.columns = layer1
    LOGGER.info("subcable layer constructure done.")
    return layer1_df, layer2_df


def one_cable_analyze(data, monitor_id, equip_id):
    data_4_train = data.loc[data['equip_id'] == equip_id]
    data_4_train = data_4_train.loc[data_4_train['monitor_id'] == str(
        monitor_id)]
    if len(data_4_train.values) == 0:
        LOGGER.warning(
            f"No data for equip[{equip_id}] & monitor[{monitor_id}] to train.")
        return
    if data_4_train.isnull().values.any():
        LOGGER.warning("Nan exists in data_4_train,dealt with 0s.")
        data_4_train.fillna(0)
    equip_name = data_4_train['equip_name'].values[0]

    x_train, y_train = data_4_train.iloc[:, 5:
                                         -2], data_4_train.iloc[:,
                                                                -1]  # 取出数据和标签
    x_train = x_train.astype('float')
    y_train = y_train.astype('int')

    model = XGBClassifier(n_estimators=100)
    model = model.fit(x_train, y_train)

    # Initialize the shapley explainer.
    explainer = shap.TreeExplainer(model)
    if len(x_train) >= 200:
        try:
            shapley_values = pd.DataFrame(
                explainer.shap_values(x_train.iloc[:200])[(y_train.iloc[0])])
        except IndexError:
            LOGGER.error(
                "Class num is less during sv calculation, dealt with Class: 0")
            shapley_values = pd.DataFrame(
                explainer.shap_values(x_train.iloc[:200])[0])
    else:
        try:
            shapley_values = pd.DataFrame(
                explainer.shap_values(x_train)[(y_train.iloc[0])])
        except IndexError:
            LOGGER.error(
                "Class num is less during sv calculation, dealt with Class: 0")
            shapley_values = pd.DataFrame(explainer.shap_values(x_train)[0])
    LOGGER.info("Shapley analysis done.")
    feature_name = pd.DataFrame(x_train).columns.tolist()  # 属性值
    # shapley_values = shapley_values * 100
    if len(shapley_values.T) == 1:
        shapley_values = pd.DataFrame(softmax(shapley_values.values).reshape(
            1, -1),
                                      index=[0])
    else:
        shapley_values = pd.DataFrame(softmax(
            shapley_values.values))  # 对shapely value做归一化，不然不好懂
    shapley_values.columns = feature_name

    # Get cengci constructor of middle two layers.
    layer1, layer2 = cengci(shapley_values)

    # Make these layers together into a dataframe.
    l0, l1, l2, l3 = [None] * len(feature_name), [None] * len(feature_name), [None] * len(feature_name), \
                     [None] * len(feature_name)
    n0, n1, n2, n3 = [None] * len(feature_name), [None] * len(feature_name), [None] * len(feature_name), \
                     feature_name
    for idx in range(len(feature_name)):
        l3[idx] = shapley_values.iloc[0, idx]

    for idx in range(len(layer2.columns.tolist())):
        l2[idx] = layer2.iloc[0, idx]
        n2[idx] = layer2.columns.tolist()[idx]

    for idx in range(len(layer1.columns.tolist())):
        l1[idx] = layer1.iloc[0, idx]
        n1[idx] = layer1.columns.tolist()[idx]

    l0[0] = y_train.values.tolist()[0]
    n0[0] = 'RiskLevel'

    # Write to sql as formatted.
    # Write into mysql with formatted layer.
    layer_constructor = {
        'electric': ['voltage', 'current', 'power'],
        'temperature': ['temperature'],
        'disturb': ['disturbance']
    }
    layer_constructor_final = {
        'voltage': ['voltage_phase_A', 'voltage_phase_B', 'voltage_phase_C'],
        'current': ['current_phase_A', 'current_phase_B', 'current_phase_C'],
        'power': ['active_power', 'reactive_power', 'power_factor'],
        'temperature': ['temperature_opti', 'temperature_cablecore'],
        'disturbance': ['disturbance_energy']
    }

    layer_2_sql = [
        f"{equip_id}", f"{equip_name}", f'{monitor_id}', "RiskLevel",
        f"{(y_train.values.tolist()[0])}", None
    ]
    write_layer_2_sql('test_subcable_tree_explain', layer_2_sql)

    for node in list(layer_constructor.keys()):
        node_id = query_node_id_4_layer('RiskLevel')
        layer_2_sql = [
            f"{equip_id}", f"{equip_name}", f'{monitor_id}', f"{node}",
            f"{layer1[f'{node}'][0]}", f"{node_id}"
        ]
        write_layer_2_sql('test_subcable_tree_explain', tuple(layer_2_sql))

        for sub_node in layer_constructor[f'{node}']:
            node_id = query_node_id_4_layer(f'{node}')
            if sub_node == 'winding_temperature' or sub_node == 'core_ground_current_data':
                layer_2_sql = [
                    f"{equip_id}", f"{equip_name}", f'{monitor_id}',
                    f"{sub_node}", f"{shapley_values[f'{sub_node}'][0]}",
                    f"{node_id}"
                ]
                write_layer_2_sql('test_subcable_tree_explain',
                                  tuple(layer_2_sql))
                continue
            else:
                layer_2_sql = [
                    f"{equip_id}", f"{equip_name}", f'{monitor_id}',
                    f"{sub_node}", f"{layer2[f'{sub_node}'][0]}", f"{node_id}"
                ]
            write_layer_2_sql('test_subcable_tree_explain', tuple(layer_2_sql))

            for sub_sub_node in layer_constructor_final[f'{sub_node}']:
                node_id = query_node_id_4_layer(f'{sub_node}')
                layer_2_sql = [
                    f"{equip_id}", f"{equip_name}", f'{monitor_id}',
                    f"{sub_sub_node}",
                    f"{shapley_values[f'{sub_sub_node}'][0]}", f"{node_id}"
                ]
                write_layer_2_sql('test_subcable_tree_explain',
                                  tuple(layer_2_sql))

    # Feature importance.
    xgb_feature_importance = model.feature_importances_
    if sum(xgb_feature_importance) == 0:
        xgb_feature_importance[True] = 1
    xgb_feature_importance = np.array(xgb_feature_importance).reshape(-1, 1)
    xgb_feature_importance = pd.DataFrame(xgb_feature_importance)
    xgb_feature_importance = xgb_feature_importance.fillna(1)
    xgb_feature_importance = xgb_feature_importance.values.reshape(-1, 1)
    # minmax_ = DataMinMax(xgb_feature_importance)
    # xgb_feature_importance = minmax_.mm_nd()
    info_4_con = np.array(
        [None, f'{equip_id}', f'{equip_name}', f'{monitor_id}'])
    temp_xgb_insert = np.concatenate(
        [info_4_con.reshape(1, -1),
         xgb_feature_importance.reshape(1, -1)],
        axis=1)

    # 特征重要度形成层次结构/加和
    vol = np.sum(xgb_feature_importance[0:3])
    cur = np.sum(xgb_feature_importance[3:6])
    power = np.sum(xgb_feature_importance[6:9])
    temper = np.sum(xgb_feature_importance[9:11])
    disturb = xgb_feature_importance[11][0]
    electric = vol + cur + power
    rest = np.array(
        [vol, cur, power, temper, disturb, electric, temper, disturb])
    temp_xgb_insert = np.concatenate(
        [temp_xgb_insert, rest.reshape(1, -1)], axis=1)
    LOGGER.info("Feature importance exported.")
    temp_xgb_insert = pd.DataFrame(temp_xgb_insert)
    df_has_nan = temp_xgb_insert.iloc[:, 1:].isnull()
    # temp_xgb_insert = temp_xgb_insert.fillna(1)
    if df_has_nan.values.any():
        LOGGER.warning(
            "Nan exiats in feature importance(most because of to many same samples), dealt with 0s."
        )
        temp_xgb_insert.fillna(1)
    write_xgb_2_sql('test_subcable_feature_importance',
                    temp_xgb_insert.values.reshape(-1).tolist())


def main():
    # Read data/header from mysql.
    sql_read = Mysql(host=mysqlConfig.HOST,
                     user=mysqlConfig.USER,
                     pwd=mysqlConfig.PASSWORD,
                     dbname=mysqlConfig.DATABASE,
                     port=mysqlConfig.PORT)
    sql_sentence = 'select * from test_subcable_monitor ORDER BY acquisition_time DESC LIMIT 20000'
    data_from_mysql, _, header = sql_read.queryOperation(sql_sentence)

    # Process to useful data.
    data_from_mysql = pd.DataFrame(list(data_from_mysql))
    data_from_mysql.columns = header[0]
    data_from_mysql.drop(columns=['apparent_power'], inplace=True)

    # Encode risk_level column.
    data_from_mysql.loc[data_from_mysql['risk_level'] == '无风险',
                         'risk_level'] = 0
    data_from_mysql.loc[data_from_mysql['risk_level'] == '低风险',
                         'risk_level'] = 1
    data_from_mysql.loc[data_from_mysql['risk_level'] == '中风险',
                         'risk_level'] = 2
    data_from_mysql.loc[data_from_mysql['risk_level'] == '高风险',
                         'risk_level'] = 3

    # Init table in mysql to getting ready.
    table_list = [
        'test_subcable_feature_importance', 'test_subcable_tree_explain',
        'test_subcable_mmd_good'
    ]
    clear_table(table_list)
    LOGGER.info("Subcable init process[delete result tables] done.")

    # Read benchmark device info.
    sql_read = Mysql(host=mysqlConfig.HOST,
                     user=mysqlConfig.USER,
                     pwd=mysqlConfig.PASSWORD,
                     dbname=mysqlConfig.DATABASE,
                     port=mysqlConfig.PORT)
    sql_sentence = 'select * from test_benchmark_normal_subcable LIMIT 1000'
    data_from_mysql_bm, _, header_bm = sql_read.queryOperation(sql_sentence)

    # DataFrame.
    data_from_mysql_bm = pd.DataFrame(list(data_from_mysql_bm))
    data_from_mysql_bm.columns = header_bm[0]

    # Get all cables' names.
    # Set() the list.
    cable_ids = list(set(data_from_mysql['equip_id'].values.tolist()))
    cable_names = np.array([
        data_from_mysql.loc[data_from_mysql['equip_id'] == i,
                            ['equip_name']].values.tolist()[0]
        for i in cable_ids
    ]).reshape(-1)
    cable_monitors = np.arange(50) + 1  # [1, 2, ..., 49, 50]

    # for id_ in cable_ids:
    #     for monitor in tqdm(cable_monitors):
    #         temp_monitor_id = id_ + '_' + str(monitor)
    #         one_cable_analyze(data_from_mysql, temp_monitor_id, id_)
    #         LOGGER.info(f'海缆{id_}的第{monitor}号监测点分析完成')

    # Select cable with the highest score.
    data_from_mysql_bm.sort_values(by='time', ascending=False, inplace=True)
    data_from_mysql_bm.sort_values(by='score', ascending=False, inplace=True)
    best_cable_id = data_from_mysql_bm.iloc[0, 1]
    best_cable_monitor_id = data_from_mysql_bm.iloc[0, 3]
    best_cable_name = data_from_mysql_bm.iloc[0, 2]

    # 找出标杆设备的数据
    # best_cable_id = 'CEPD_HL_A'     # 数据太少，只能先这样调试
    best_cable_data = data_from_mysql.loc[data_from_mysql['equip_id'] ==
                                          best_cable_id]
    best_cable_data = best_cable_data.loc[best_cable_data['monitor_id'] ==
                                          best_cable_monitor_id]

    drop_column = [
        'id', 'equip_id', 'equip_name', 'monitor_id', 'acquisition_time',
        'comprehensive_score', 'risk_level'
    ]  # 迭代的取出数据后，要将这些列drop掉再开始对比
    best_cable_data = best_cable_data.drop(columns=drop_column)
    feature_names = best_cable_data.columns.tolist()

    for id_ in cable_ids:
        temp_cable_data = data_from_mysql.loc[data_from_mysql['equip_id'] ==
                                              id_]
        temp_cable_name = temp_cable_data['equip_name'].values[0]
        for monitor in tqdm(cable_monitors):
            temp_monitor_id = id_ + '_' + str(monitor)
            temp_data = temp_cable_data.loc[temp_cable_data['monitor_id'] ==
                                            temp_monitor_id]
            temp_data = temp_data.drop(columns=drop_column)
            mmd_compare(temp_data, best_cable_data, feature_names, id_,
                        temp_cable_name, monitor, best_cable_id,
                        best_cable_name, best_cable_monitor_id)
    LOGGER.info("mmd analysis done.")


def parse_arg():
    parse = argparse.ArgumentParser(description='andian cause gis args')
    parse.add_argument("-t",
                       "--time",
                       type=int,
                       default=1,
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
        main()
        print("subcable致因分析完成")
        time.sleep(args.time * 60)
