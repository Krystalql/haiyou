import db
import numpy as np
import datetime


class DAO:
    # 初始化函数，初始化连接列表
    def __init__(self, args):
        self.mysql1 = db.MYSQL(host=args.host,
                               port = args.port,
                               user=args.user,
                               pwd=args.password,
                               dbname=args.database)
        #host(str): MySQL服务器地址
        #port(int): MySQL服务器端口号
        #user(str): 用户名
        #passwd(str): 密码
        #db(str): 数据库名称
        #charset(str): 连接编码
        # self.mysql2 = db.MYSQL(host='47.108.51.23',
        #                        user='root',
        #                        pwd='data@123456',
        #                        dbname='dqjc_zj_business')
        self.mysql1.getConnection()
        # self.mysql2.getConnection()

    def queryNormData2(self, equip_id):
        sql = " select pd_peak_Max,\
                    discharge_amplitude_Max,\
                    gis_pressure_Max,\
                    micro_water_content_Max,\
                    gis_temperature_Max,\
                    score_Max,\
                    pd_peak_Min,\
                    discharge_amplitude_Min,\
                    gis_pressure_Min,\
                    micro_water_content_Min,\
                    gis_temperature_Min,\
                    score_Min\
                    from andian_gis_train_data\
                    where equip_id = %s \
                    ORDER BY train_time desc limit 1"

        dataList, row = self.mysql1.queryOperation(sql, (equip_id))
        # print(dataList)
        result = list(dataList[0])
        # print(result)
        # 返回[1*14]的列表
        return result

    def queryGISInfo(self):
        sql = "select distinct equip_id,equip_name from andian_gis_monitor"
        #where equip_type_id = %s  设备类型 选择GIS
        dataList, row = self.mysql1.queryOperation(sql, ())  # 9是GIS
        # print(dataList)
        return dataList

    def queryVariable(self, tid, step):  #query查询 Variable变量
        sql = "select input_time,\
               pd_peak,\
               discharge_amplitude,\
               gis_pressure,\
               micro_water_content,\
               gis_temperature\
               from andian_gis_monitor\
               where equip_id=%s ORDER BY input_time desc limit %s"

        dataList, row = self.mysql1.queryOperation(
            sql, (tid, step))  #queryOperation数据库查询函数 返回一个二维元组和行数
        #tid作为输入参数，equip_id=tid,
        #step设置取几步，acquisition_time desc limit step
        if len(dataList) == 0:
            return dataList, None
        ti = dataList[0][0]  #取出时间
        ti = ti.replace(second=0)  #将时间的秒置零
        result = []
        for x in reversed(
                dataList
        ):  #reversed()函数的作用是返回一个反转(倒序)的（元组、列表、字符串、range）。反转的原因：最新的时间点在最下面，要把它倒过来。
            result.append(x[1:])  #x[1:]第一位是时间，从第一位后开始读，不读取时间。
        result = np.array(result, dtype=float)  #np.array 化为矩阵
        # 返回array.[step*7]
        return result, ti  #返回变压器信息矩阵，12行，每行为七个变量的信息
        #tid为第一个点的时间

    def queryTime(self, tid, step):  #查询时间
        sql = "select input_time from andian_gis_predict where equip_id = %s  ORDER BY input_time desc limit %s"
        dataList, row = self.mysql1.queryOperation(sql, (tid, step))
        if len(dataList) == 0:
            return None
        ti = dataList[0][0] + datetime.timedelta(
            minutes=-180)  ##返回第一个时间点的前180分钟？？??
        return ti

    # 定义变压器单变量预测历史表删除函数
    def deleteSingleVariable(self, tid, step):
        sql = "delete from andian_gis_predict where equip_id = %s  ORDER BY input_time desc limit %s"
        self.mysql1.deleteOperation(sql, (tid, step))

    def insertVariable(self, tid, equipName, data, ti):
        sql = "insert into andian_gis_predict (`equip_id`,\
                      `equip_name`,\
                      `input_time`,\
                      `pd_peak`,\
                      `discharge_amplitude`,\
                      `gis_pressure`,\
                      `micro_water_content`,\
                      `gis_temperature`,\
                      `pd_peak_norm`,\
                      `discharge_amplitude_norm`,\
                      `gis_pressure_norm`,\
                      `micro_water_content_norm`,\
                      `gis_temperature_norm`,\
                      `comprehensive_score`,\
                      `risk_level`)\
                      values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"                                                                             #21个
        formatData = []
        for temp in data:
            ti = ti + datetime.timedelta(minutes=30)  ##得到30分钟后的时间点？？??
            ti2 = ti.strftime(
                "%Y-%m-%d %H:%M:%S")  #.strftime转换为特定的时间格式 如：2021 9 25 17：11：30
            #%Y 四位数的年份表示（000-9999）
            #%m 月份（01-12）
            #%d 月内中的一天（0-31）
            #%H 24小时制小时数（0-23）%M 分钟数（00=59）%S 秒（00-59）
            info = (tid, equipName, ti2)
            # print("info",info)
            formatData.append(info + tuple(temp.tolist()))  #.tolist转化为列表
            # print("formatData",formatData)

        rowcount = self.mysql1.BatchInsertOperation(sql, formatData)
        if rowcount == 0:
            print("插入失败")

    # 定义插入模型图的参数
    def insertModelParam(self, equip_id, equip_name, paramNumber, data):
        para = "update andian_predict_gis_model_1" + " set "
        for j in range(1, paramNumber + 1):
            #用循环写sql语句
            #dbNumber=(循环数+1)=1或2 用于确定数据集  update test_predict_tf_model_1、
            #paramNumber=18或6
            para += "parameter" + str(j) + " =%s,"
        sql = para[:-1] + " where equip_id = %s "
        #sql=update test_predict_tf_model_str(dbNumber) set parameter1 =%s,parameter2 =%s,parameter3 =%s,parameter4 =%s,parameter5 =%s,parameter6 =%s where equip_id = %s

        affectedRows = self.mysql1.updateOperation(sql,
                                                   tuple(data) + (equip_id, ))
        if affectedRows > 0:  # 变压器存在，更新成功。
            return
        else:  # 变压器不存在，插入数据
            para = "insert into andian_predict_gis_model_1" + " (equip_id,equip_name,"
            s = " values (%s,%s,"
            for i in range(1, paramNumber + 1):
                #利用循环写sql语句
                para += "parameter" + str(i) + ","
                s += "%s,"
            sql = para[:-1] + ")" + s[:-1] + ")"
            #sql=insert into test_predict_tf_model_1 (equip_id,equip_name,parameter1,parameter2,parameter3,parameter4,parameter5,parameter6) values (%s,%s,%s,%s,%s,%s,%s,%s)
            # print('sql:',sql)
            self.mysql1.insertOperation(sql,
                                        (equip_id, equip_name) + tuple(data))

    def closeConnection(self):
        self.mysql1.closeConnection()
        # self.mysql2.closeConnection()


if __name__ == '__main__':
    dao = DAO()
    q = dao.queryTime("CEPD-B01", 1)
    print(q)
