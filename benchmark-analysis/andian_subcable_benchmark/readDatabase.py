import pandas as pd
import pymysql


def read_target_time(config, device_type):
    if device_type == "变压器":
        sql = '''select acquisition_time from test_transformer_monitor_average 
        ORDER BY acquisition_time DESC LIMIT 0, 1'''

    else:
        sql = '''select acquisition_time from test_subcable_monitor_average 
        ORDER BY acquisition_time DESC LIMIT 0, 1'''
        # 创建连接
    try:
        db = pymysql.connect(**config)
        cursor = db.cursor()  # 创建一个游标对象
    except:
        print("数据库连接错误")
        return 0, 1

    # 读数据
    try:
        cursor.execute(sql)
        data = cursor.fetchall()
        data = pd.DataFrame(list(data))
    except:
        db.rollback()
        print(device_type + "数据库读取错误")
        db.close()
        return 0, 2
    db.close()

    return data, 0


def read_time(config, device_type):
    if device_type == "变压器":
        sql1 = '''select time from test_benchmark_normal_transformer 
        ORDER BY time DESC LIMIT 0, 1'''

        sql2 = '''select time from test_benchmark_abnormal_transformer 
              ORDER BY time DESC LIMIT 0, 1'''

    else:
        sql1 = '''select time from test_benchmark_normal_subcable 
        ORDER BY time DESC LIMIT 0, 1'''

        sql2 = '''select time from test_benchmark_abnormal_subcable
                ORDER BY time DESC LIMIT 0, 1'''
        # 创建连接
    try:
        db = pymysql.connect(**config)
        cursor = db.cursor()  # 创建一个游标对象
    except:
        print("数据库连接错误")
        return 0, 1

    # 读数据
    try:
        cursor.execute(sql1)
        data1 = cursor.fetchall()
        data1 = pd.DataFrame(list(data1))

        cursor.execute(sql2)
        data2 = cursor.fetchall()
        data2 = pd.DataFrame(list(data2))
    except:
        db.rollback()
        print(device_type + "数据库读取错误")
        db.close()
        return 0, 2
    db.close()

    return (data1, data2), 0


def read_device(config, device_type):

    if device_type == "变压器":

        sql = '''select equip_id, voltage_max, current_max, apparent_power, ae_partial_discharge, 
        rf_partial_discharge, core_ground_current_data, winding_temperature, risk_level 
        from
        (select * from test_transformer_monitor_average ORDER BY acquisition_time DESC LIMIT 9999) t 
        GROUP BY t.equip_id 
        ORDER BY t.acquisition_time desc;'''

    else:
        sql = '''select equip_id, equip_name, monitor_id, voltage_max, current_max, apparent_power, 
        temperature_opti, temperature_cablecore, disturbance_energy, risk_level
        from
        (select * from test_subcable_monitor_average ORDER BY acquisition_time DESC LIMIT 9999) t 
        GROUP BY t.monitor_id 
        ORDER BY t.acquisition_time DESC;'''

    # 创建连接
    try:
        db = pymysql.connect(**config)
        cursor = db.cursor()  # 创建一个游标对象
    except:
        print("数据库连接错误")
        return 0, 1

    # 读数据
    try:
        cursor.execute(sql)
        data = cursor.fetchall()
        data = pd.DataFrame(list(data))
    except:
        db.rollback()
        print(device_type + "数据库读取错误")
        db.close()
        return 0, 2

    db.close()

    return data, 0


def read_weight(config, device_type):
    if device_type == "变压器":

        sql = '''select voltage_weight, current_weight, power_weight, ae_partial_discharge_weight, 
              rf_partial_discharge_weight, core_grounding_current_weight, winding_temperature_weight
              from test_transformer_weight'''
    else:
        sql = '''select voltage_weight, current_weight, power_weight, temperature_opti_weight, 
              temperature_cablecore_weight, disturbance_energy_weight from test_subcable_weight'''

    # 创建连接
    try:
        db = pymysql.connect(**config)
        cursor = db.cursor()  # 创建一个游标对象
    except:
        print("数据库连接错误")
        return 0, 1

    # 读数据
    try:
        cursor.execute(sql)
        data = cursor.fetchall()
        data = pd.DataFrame(list(data))
    except:
        db.rollback()
        print(device_type + "数据库读取错误")
        db.close()
        return 0, 2

    db.close()

    return data, 0
