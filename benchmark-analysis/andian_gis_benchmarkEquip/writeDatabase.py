import pymysql


def write_result(config, normal_data, abnormal_data, device_type):

    try:
        db = pymysql.connect(**config)
        cursor = db.cursor()  # 创建一个游标对象
    except:
        print("数据库连接错误")
        return 1

    normal_table = 'andian_benchmark_normal'
    abnormal_table = 'andian_benchmark_abnormal'

    if device_type == "GIS":
        normal_table += "_gis"
        abnormal_table += "_gis"
    else:
        normal_table += "_subcable"
        abnormal_table += "_subcable"

    if normal_data is not None:
        if device_type == "GIS":
            try:
                cursor.executemany("insert into " + normal_table + " values(%s,%s,%s,%s,%s,%s)", normal_data)
                db.commit()
            except:
                print(device_type + "数据库写入结果错误")
                db.rollback()
                db.close()
                return 2
        else:
            try:
                cursor.executemany("insert into " + normal_table + " values(%s,%s,%s,%s,%s,%s,%s)", normal_data)
                db.commit()
            except:
                print(device_type + "数据库写入结果错误")
                db.rollback()
                db.close()
                return 2

    # if abnormal_data is not None:
    #     if device_type == "GIS":
    #         try:
    #             cursor.executemany("insert into " + abnormal_table + " values(%s,%s,%s,%s,%s,%s)", abnormal_data)
    #             db.commit()
    #         except:
    #             print(device_type + "数据库写入结果错误")
    #             db.rollback()
    #             db.close()
    #             return 2
    #     else:
    #         try:
    #             cursor.executemany("insert into " + abnormal_table + " values(%s,%s,%s,%s,%s,%s,%s)", abnormal_data)
    #             db.commit()
    #         except:
    #             print(device_type + "数据库写入结果错误")
    #             db.rollback()
    #             db.close()
    #             return 2

    db.close()
    return 0


# def write_weight(config, weight, device_type):
#     try:
#         db = pymysql.connect(**config)
#         cursor = db.cursor()  # 创建一个游标对象
#     except:
#         print("权重数据库连接错误")
#         return 1
#
#     if device_type == "变压器":
#
#         sql = "insert ignore into test_transformer_weight values(%s,%s,%s,%s,%s,%s,%s,%s,%s)"
#         try:
#             cursor.execute(sql, [1, 0, 1, 1, 1, 1, 1, 1, 1])
#             cursor.execute(sql, [2, 1] + weight.tolist())
#             db.commit()
#         except:
#             db.rollback()
#             print(device_type + "数据库写入权重错误")
#             db.close()
#             return 2
#
#     else:
#         sql = "insert ignore into test_subcable_weight values(%s,%s,%s,%s,%s,%s,%s,%s)"
#         try:
#             cursor.execute(sql, [1, 0, 1, 1, 1, 1, 1, 1])
#             cursor.execute(sql, [2, 1] + weight.tolist())
#             db.commit()
#         except:
#             db.rollback()
#             print(device_type + "数据库写入权重错误")
#             db.close()
#             return 2
#     db.close()
#     return 0
#
#
# def write_visualization(config, data, device_type):
#     try:
#         db = pymysql.connect(**config)
#         cursor = db.cursor()  # 创建一个游标对象
#     except:
#         print("可视化数据库连接错误")
#         return 1
#
#     if device_type == "变压器":
#         table_name = "test_transformer_visualization"
#     else:
#         table_name = "test_subcable_visualization"
#
#     sql = "insert into " + table_name + " values(%s,%s,%s,%s,%s,%s,%s)"
#
#     try:
#         cursor.executemany(sql, data)
#         db.commit()
#     except:
#         db.rollback()
#         print(device_type + "数据库写入可视化错误")
#         db.close()
#         return 2
#
#     db.close()
#
#     return 0