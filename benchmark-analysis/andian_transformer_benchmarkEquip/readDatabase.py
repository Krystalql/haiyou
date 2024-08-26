import pandas as pd
import pymysql


def read_target_time(config, device_type):
    if device_type == "变压器":
        sql = '''select acquisition_time from andian_transformer_monitor_average 
        ORDER BY acquisition_time DESC LIMIT 0, 1'''

    else:
        sql = '''select acquisition_time from andian_transformer_monitor_average 
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
        sql1 = '''select time from andian_benchmark_normal_transformer 
        ORDER BY time DESC LIMIT 0, 1'''

        sql2 = '''select time from andian_benchmark_abnormal_transformer 
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

        sql = '''select equip_id, equip_name, core_ground_current_data, winding_temperature, H2_ppm, C2H4_ppm, C2H2_ppm,
        C2H6_ppm, CH4_ppm, CO_ppm, CO2_ppm, total_hydrocarbon_ppm, micro_water_ppm, risk_level 
        from
        (select * from andian_transformer_monitor_average ORDER BY acquisition_time DESC LIMIT 30) t 
        GROUP BY t.equip_id, t.equip_name,t.core_ground_current_data, t.winding_temperature, t.H2_ppm, t.C2H4_ppm, t.C2H2_ppm,t.acquisition_time,
        t.C2H6_ppm, t.CH4_ppm, t.CO_ppm, t.CO2_ppm, t.total_hydrocarbon_ppm, t.micro_water_ppm, t.risk_level 
        ORDER BY t.acquisition_time desc;'''

    else:
        sql = '''select equip_id, equip_name, voltage_max, current_max, apparent_power, ae_partial_discharge, 
        rf_partial_discharge, core_ground_current_data, winding_temperature, risk_level, H2_ppm, C2H4_ppm, CH4_ppm,
        CO_ppm, CO2_ppm, total_hydrocarbon_ppm, micro_water_ppm 
        from
        (select * from andian_transformer_monitor_average ORDER BY acquisition_time DESC LIMIT 9999) t 
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
    # try:
    cursor.execute(sql)
    data = cursor.fetchall()
    data = pd.DataFrame(list(data))
    # except:
    #     db.rollback()
    #     print(device_type + "数据库读取错误")
    #     db.close()
    #     return 0, 2

    db.close()

    return data, 0


def read_weight(config, device_type):
    if device_type == "变压器":

        sql = '''select grounding_current, winding_temperature, oil_chromatography from andian_transformer_weight'''
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
