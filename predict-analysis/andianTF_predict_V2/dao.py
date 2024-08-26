import db
import numpy as np
import datetime


class DAO:
    # 初始化函数，初始化连接列表
    def __init__(self, args):
        self.mysql1 = db.MMYSQL(host=args.host,
                               port=args.port,
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


        # 定义查询归一化数值函数  test_transfromer_train_data是从总库中查询，没有区分每个变压器。

    def queryNormData2(self, equip_id):
        sql = " select \
                    current_Max,\
                    active_power_Max,\
                    reactive_power_Max,\
                    winding_temperature_Max,\
                    core_grounding_current_Max,\
                    H2_ppm_Max,\
                    C2H4_ppm_Max,\
                    C2H2_ppm_Max,\
                    C2H6_ppm_Max,\
                    CH4_ppm_Max,\
                    CO_ppm_Max,\
                    CO2_ppm_Max,\
                    total_hydrocarbon_ppm_Max,\
                    micro_water_ppm_Max,\
                    score_Max,\
                    current_Min,\
                    active_power_Min,\
                    reactive_power_Min,\
                    winding_temperature_Min,\
                    core_grounding_current_Min,\
                    H2_ppm_Min,\
                    C2H4_ppm_Min,\
                    C2H2_ppm_Min,\
                    C2H6_ppm_Min,\
                    CH4_ppm_Min,\
                    CO_ppm_Min,\
                    CO2_ppm_Min,\
                    total_hydrocarbon_ppm_Min,\
                    micro_water_ppm_Min,\
                    score_Min\
                    from andian_transformer_train_data\
                    where equip_id = %s\
                    ORDER BY train_time desc limit 1"

        dataList, row = self.mysql1.queryOperation(sql, (equip_id, ))
        # print(dataList)
        result = list(dataList[0])
        return result

    # def queryMaxData(self):
    #     sql = " select equip_id,\
    #                    voltage_max,\
    #                    current_max,\
    #                    apparent_power_max,\
    #                    winding_temperature_max,\
    #                    ae_partial_discharge_max,\
    #                    rf_partial_discharge_max,\
    #                    core_grounding_current_max\
    #             from test_transformer_maxData"
    #     dataList, row = self.mysql1.queryOperation(sql, ())
    #     dataList = np.array(dataList)
    #     maxDataDict = {}
    #     for x in dataList:
    #         maxDataDict[x[0]] = x[1:].astype(np.float)
    #     return maxDataDict

    # def insertWarnResult(self, equip_id, data, equipName):
    #     sql = "select equip_id from test_transformer_gaojingzhi \
    #           where equip_id = %s"
    #     result, row = self.mysql1.queryOperation(sql, (equip_id,))
    #     if result:
    #         sql = "update test_transformer_gaojingzhi set \
    #                         `voltage_max`=%s,\
    #                         `current_max`=%s,\
    #                         `apparent_power_max`=%s,\
    #                         `winding_temperature_max`=%s,\
    #                         `ae_partial_discharge_max`=%s,\
    #                         `rf_partial_discharge_max`=%s,\
    #                         `core_grounding_current_max`=%s\
    #                         where equip_id = %s"
    #         self.mysql1.updateOperation(sql, tuple(data.tolist()) + (equip_id,))
    #     else:
    #         sql = "insert into test_transformer_gaojingzhi (`equip_id`,\
    #                         `equip_name`,\
    #                         `voltage_max`,\
    #                         `current_max`,\
    #                         `apparent_power_max`,\
    #                         `winding_temperature_max`,\
    #                         `ae_partial_discharge_max`,\
    #                         `rf_partial_discharge_max`,\
    #                         `core_grounding_current_max`)\
    #                         values (%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    #         row = self.mysql1.insertOperation(sql, (equip_id, equipName) + tuple(data.tolist()))
    #         if row == 0:
    #             print("插入失败")

    # def queryWarnData(self, equip_id):
    #     sql = "select rated_voltage_high,\
    #                     rated_current_high,\
    #                     rated_capacity\
    #                     from eem_transformer_info\
    #                     where equip_id = %s"
    #     dataList1, row = self.mysql2.queryOperation(sql, (equip_id,))
    #     sql = "select winding_temperature_base,\
    #                   ultrasonic_partial_discharge_base,\
    #                   hf_partial_discharge_base,\
    #                   core_ground_current_base\
    #                   from eem_transformer_configuration\
    #                   where equip_id = %s"
    #     dataList2, row = self.mysql2.queryOperation(sql, (equip_id,))
    #     if len(dataList1) == 0 or len(dataList2) == 0:
    #         print(equip_id+"设备的告警值为空，请到eem_transformer_info和eem_transformer_configuration数据库及时完善")
    #         data = []
    #     else:
    #         dataList1 = np.array(dataList1[0], dtype=float)
    #         dataList2 = np.array(dataList2[0], dtype=float)
    #         data = np.concatenate([dataList1, dataList2], axis=0)
    #     return data

    def queryTransformerInfo(self):  #从数据库中获取变压器信息
        sql = "select distinct equip_id,equip_name from andian_transformer_monitor_average"
        # equip_id,equip_name ; eem_electrical_equipment_account     = %s???
        dataList, row = self.mysql1.queryOperation(
            sql,
            ())  #queryOperation数据库查询函数 (0,)这里的0给上面的 equip_type_id = 0，查询变压器
        return dataList  #返回一个列表  ？？？???查询到的变压器信息是什么格式呢？

    def queryVariable(self, tid, step):  #query查询 Variable变量
        sql = "select acquisition_time,\
              current_max,\
              active_power, \
              reactive_power, \
              winding_temperature,\
              core_ground_current_data,\
              H2_ppm,\
              C2H4_ppm,\
              C2H2_ppm,\
              C2H6_ppm,\
              CH4_ppm,\
              CO_ppm,\
              CO2_ppm,\
              total_hydrocarbon_ppm,\
              micro_water_ppm\
              from andian_transformer_monitor_average\
              where equip_id=%s ORDER BY acquisition_time desc limit %s"

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
        sql = "select acquisition_time from andian_transformer_predict where equip_id = %s ORDER BY acquisition_time desc limit %s"
        dataList, row = self.mysql1.queryOperation(sql, (tid, step))
        if len(dataList) == 0:
            return None
        ti = dataList[0][0] + datetime.timedelta(
            minutes=-180)  ##返回第一个时间点的前180分钟？？??
        return ti

    # 定义变压器单变量预测历史表删除函数
    def deleteSingleVariable(self, tid, step):
        sql = "delete from andian_transformer_predict where equip_id = %s ORDER BY acquisition_time desc limit %s"
        self.mysql1.deleteOperation(sql, (tid, step))

    # 定义变压器预测历史表插入函数
    # def insertVariable(self, tid, equipName, data, ti):
    #     #     sql = "insert into test_transformer_predict (`equip_id`,\
    #     #             `equip_name`,\
    #     #             `acquisition_time`,\
    #     #             `is_deleted`,\
    #     #             `voltage`,\
    #     #             `current`,\
    #     #             `apparent_power`,\
    #     #             `winding_temperature`,\
    #     #             `ae_partial_discharge`,\
    #     #             `rf_partial_discharge`,\
    #     #             `core_grounding_current`,\
    #     #             `voltage_norm`,\
    #     #             `current_norm`,\
    #     #             `apparent_power_norm`,\
    #     #             `wind_temp_norm`,\
    #     #             `ae_pd_norm`,\
    #     #             `rf_pd_norm`,\
    #     #             `core_gc_norm`,\
    #     #             `score`,\
    #     #             `rank`,\
    #     #             `voltage_norm_warn`,\
    #     #             `current_norm_warn`,\
    #     #             `app_power_norm_warn`,\
    #     #             `wind_temp_norm_warn`,\
    #     #             `ae_pd_norm_warn`,\
    #     #             `rf_pd_norm_warn`,\
    #     #             `core_gc_norm_warn`)\
    #     #             values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    #     #     formatData = []
    #     #     for temp in data:
    #     #         ti = ti + datetime.timedelta(minutes=30)
    #     #         ti2 = ti.strftime("%Y-%m-%d %H:%M:%S")
    #     #         info = (tid, equipName, ti2, '0')
    #     #         formatData.append(info + tuple(temp.tolist()))
    #     #     rowcount = self.mysql1.BatchInsertOperation(sql, formatData)
    #     #     if rowcount == 0:
    #     #         print("插入失败")

    def insertVariable(self, tid, equipName, data, ti):
        sql = "insert into andian_transformer_predict (`equip_id`,\
                      `equip_name`,\
                      `acquisition_time`,\
                      `current`,\
                      `active_power`, \
                      `reactive_power`, \
                      `winding_temperature`,\
                      `core_ground_current_data`,\
                      `H2_ppm`,\
                      `C2H4_ppm`,\
                      `C2H2_ppm`,\
                      `C2H6_ppm`,\
                      `CH4_ppm`,\
                      `CO_ppm`,\
                      `CO2_ppm`,\
                      `total_hydrocarbon_ppm`,\
                      `micro_water_ppm`,\
                      `comprehensive_score`,\
                      `risk_level`,\
                      `current_norm`,\
                      `active_power_norm`, \
                      `reactive_power_norm`, \
                      `winding_temperature_norm`,\
                      `core_ground_current_data_norm`,\
                      `H2_ppm_norm`,\
                      `C2H4_ppm_norm`,\
                      `C2H2_ppm_norm`,\
                      `C2H6_ppm_norm`,\
                      `CH4_ppm_norm`,\
                      `CO_ppm_norm`,\
                      `CO2_ppm_norm`,\
                      `total_hydrocarbon_ppm_norm`,\
                      `micro_water_ppm_norm`)\
                      values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"

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
            formatData.append(info + tuple(temp.tolist()))  #.tolist转化为列表

        rowcount = self.mysql1.BatchInsertOperation(sql, formatData)
        # print("formatData:",formatData)
        if rowcount == 0:
            print("插入失败")

    # 定义插入模型图的参数
    def insertModelParam(self, equip_id, equip_name, dbNumber, paramNumber,
                         data):
        para = "update andian_predict_tf_model_1" + " set "
        for j in range(1, paramNumber + 1):
            #用循环写sql语句
            #dbNumber=(循环数+1)=1或2 用于确定数据集  update test_predict_tf_model_1、
            #paramNumber=18或6
            para += "parameter" + str(j) + " =%s,"
        sql = para[:-1] + " where equip_id = %s"
        #sql=update test_predict_tf_model_str(dbNumber) set parameter1 =%s,parameter2 =%s,parameter3 =%s,parameter4 =%s,parameter5 =%s,parameter6 =%s where equip_id = %s

        affectedRows = self.mysql1.updateOperation(sql,
                                                   tuple(data) + (equip_id, ))
        if affectedRows > 0:  # 变压器存在，更新成功。
            return
        else:  # 变压器不存在，插入数据
            para = "insert into andian_predict_tf_model_1" + " (equip_id,equip_name,"
            s = " values (%s,%s,"
            for i in range(1, paramNumber + 1):
                #利用循环写sql语句
                para += "parameter" + str(i) + ","
                s += "%s,"
            sql = para[:-1] + ")" + s[:-1] + ")"
            #sql=insert into test_predict_tf_model_1 (equip_id,equip_name,parameter1,parameter2,parameter3,parameter4,parameter5,parameter6) values (%s,%s,%s,%s,%s,%s,%s,%s)
            self.mysql1.insertOperation(sql,
                                        (equip_id, equip_name) + tuple(data))

    # def insertModelParam(self, dbNumber, paramNumber, data):
    #     para = "insert into test_predict_model" + str(dbNumber) + " ("
    #     s = " values ("
    #     for i in range(1, paramNumber + 1):
    #         para += "parameter" + str(i) + ","
    #         s += "%s,"
    #     sql = para[:-1] + ")" + s[:-1] + ")"
    #     self.mysql1.insertOperation(sql, tuple(data))

    def closeConnection(self):
        self.mysql1.closeConnection()
        # self.mysql2.closeConnection()


if __name__ == '__main__':
    dao = DAO()
    q = dao.queryTime("CEPD-B01", 1)
    print(q)
