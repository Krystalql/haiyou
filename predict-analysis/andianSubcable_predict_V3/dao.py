import db
import numpy as np
import datetime


class DAO:

    def __init__(self, args):
        self.mysql1 = db.MYSQL(host=args.host,
                               port=args.port,
                               user=args.user,
                               pwd=args.password,
                               dbname=args.database)
        self.mysql1.getConnection()

    # 定义查询归一化数值函数
    def queryNormData(self):
        sql = " select subcableVoltage_Max,\
                subcableCurrent_Max,\
                subcablePower_Max,\
                subcableTemperature_opti_Max,\
                subcableTemperature_cablecore_Max,\
                subcableDisturbance_energy_Max,\
                score_Max,\
                subcableVoltage_Min,\
                subcableCurrent_Min,\
                subcablePower_Min,\
                subcableTemperature_opti_Min,\
                subcableTemperature_cablecore_Min,\
                subcableDisturbance_energy_Min,\
                score_Min\
                from test_subcable_train_data\
                ORDER BY train_time desc limit 1"
        dataList, row = self.mysql1.queryOperation(sql, ())
        result = list(dataList[0])
        # 返回[1*16]的列表
        return result

    def queryMaxData(self, equip_id):
        sql = " select monitor_id,\
                       voltage_max,\
                       current_max,\
                       apparent_power_max,\
                       temperature_opti_max,\
                       temperature_cablecore_max,\
                       disturbance_energy_max\
                from test_subcable_maxdata\
                where equip_id = %s"
        dataList, row = self.mysql1.queryOperation(sql, (equip_id,))
        dataList = np.array(dataList)
        maxDataDict = {}
        for x in dataList:
            maxDataDict[x[0]] = x[1:].astype(np.float)
        if len(maxDataDict) == 0:
            print(equip_id+"为新海缆，请及时训练")
        return maxDataDict

    def queryWarnData(self, equip_id):
        sql = "select rated_voltage\
               from eem_subcable_info\
               where equip_id = %s"
        dataList1, row = self.mysql1.queryOperation(sql, (equip_id,))
        sql = "select section_id,\
                      max_current_rating,\
                      temperature_alarm,\
                      disturbance_threshold_one\
                      from eem_subcable_section_configuration\
                      where equip_id = %s"
        dataList2, row = self.mysql1.queryOperation(sql, (equip_id,))

        if len(dataList1) == 0 or len(dataList2) == 0:
            print(equip_id + "设备的告警值为空，请到eem_subcable_info和eem_subcable_section_configuration数据库及时完善")
            return {}
        else:
            voltage = np.float(dataList1[0][0])
            dataList2 = np.array(dataList2, dtype=float)
            sectionDict = {}
            warnData = np.ones((len(dataList2), 7), dtype=float)
            warnData[:, 0] = dataList2[:, 0]  # 区段id
            warnData[:, 1] = warnData[:, 1] * voltage  # 电压
            warnData[:, 2] = dataList2[:, 1]  # 电流
            warnData[:, 3] = dataList2[:, 1] * voltage  # 功率
            warnData[:, 4] = dataList2[:, 2]  # 绕温
            warnData[:, 5] = dataList2[:, 2]  # 缆芯温度
            warnData[:, 6] = dataList2[:, 3]  # 扰动能量
            for x in warnData:
                sectionDict[str(int(x[0]))] = x[1:]
        return sectionDict

    def querySubCableInfo(self):
        sql = "select distinct equip_id,equip_name from eem_electrical_equipment_account where equip_type_id = %s "
        dataList, row = self.mysql1.queryOperation(sql, (1,))
        return dataList

    def querySubCableKeySector(self, equip_id):
        sql = "select distinct section_id, monitor_id from test_subcable_key_sector where equip_id = %s "
        dataList, row = self.mysql1.queryOperation(sql, (equip_id,))
        sectionDict = {}
        for x in dataList:
            if x[0] not in sectionDict.keys():
                sectionDict[x[0]] = []
                sectionDict[x[0]].append(x[1])
            else:
                sectionDict[x[0]].append(x[1])
        return sectionDict

    def queryVariable(self, tid, monitor_id, step):
        sql = "select acquisition_time,\
              voltage_max,\
              current_max,\
              apparent_power,\
              temperature_opti,\
              temperature_cablecore,\
              disturbance_energy\
              from test_subcable_monitor_average\
              where equip_id= %s and monitor_id= %s ORDER BY acquisition_time desc limit %s"
        dataList, row = self.mysql1.queryOperation(sql, (tid, monitor_id, step))
        if len(dataList) == 0:
            return dataList, None
        ti = dataList[0][0]
        ti = ti.replace(second=0)
        result = []
        for x in reversed(dataList):
            result.append(x[1:])
        result = np.array(result, dtype=float)
        # 返回array.[step*7]
        return result, ti

    def queryTime(self, equip_id, monitor_id, step):
        sql = "select acquisition_time from test_subcable_predict where equip_id = %s and monitor_id = %s\
        ORDER BY acquisition_time desc limit %s"
        dataList, row = self.mysql1.queryOperation(sql, (equip_id, monitor_id, step))
        if len(dataList) == 0:
            return None
        ti = dataList[0][0] + datetime.timedelta(minutes=-180)
        return ti

    # 定义变压器单变量预测历史表删除函数
    def deleteSingleVariable(self, tid, monitor_id, step):
        sql = "delete from test_subcable_predict where equip_id = %s and monitor_id = %s ORDER BY acquisition_time desc limit %s"
        self.mysql1.deleteOperation(sql, (tid, monitor_id, step))

    def insertWarnResult(self, equip_id, monitor_id, data, equipName):
        sql = "select monitor_id from test_subcable_gaojingzhi \
              where equip_id = %s and monitor_id = %s"
        result, row = self.mysql1.queryOperation(sql, (equip_id, monitor_id))
        if result:
            sql = "update test_subcable_gaojingzhi set \
                            `voltage_max`=%s,\
                            `current_max`=%s,\
                            `apparent_power_max`=%s,\
                            `temperature_opti_max`=%s,\
                            `temperature_cablecore_max`=%s,\
                            `disturbance_energy_max`=%s\
                            where equip_id = %s and monitor_id = %s"
            self.mysql1.updateOperation(sql, tuple(data.tolist()) + (equip_id, monitor_id))
        else:
            sql = "insert into test_subcable_gaojingzhi (`equip_id`,\
                            `monitor_id`,\
                            `equip_name`,\
                            `voltage_max`,\
                            `current_max`,\
                            `apparent_power_max`,\
                            `temperature_opti_max`,\
                            `temperature_cablecore_max`,\
                            `disturbance_energy_max`)\
                            values (%s,%s,%s,%s,%s,%s,%s,%s,%s)"
            row = self.mysql1.insertOperation(sql, (equip_id, monitor_id, equipName) + tuple(data.tolist()))
            if row == 0:
                print("插入失败")

    # 定义变压器预测历史表插入函数
    def insertVariable(self, tid, equipName, monitor_id, data, ti):
        sql = "insert into test_subcable_predict (equip_id,\
                `equip_name`,\
                `monitor_id`,\
                `acquisition_time`,\
                `is_deleted`,\
                `voltage`,\
                `current`,\
                `apparent_power`,\
                `temperature_opti`,\
                `temperature_cablecore`,\
                `disturbance_energy`,\
                `voltage_norm`,\
                `current_norm`,\
                `apparent_power_norm`,\
                `temperature_opti_norm`,\
                `temperature_core_norm`,\
                `disturb_energy_norm`,\
                `score`,\
                `rank`,\
                `voltage_norm_warn`,\
                `current_norm_warn`,\
                `app_power_norm_warn`,\
                `temp_opti_norm_warn`,\
                `temp_core_norm_warn`,\
                `disturb_energy_norm_warn`)\
                values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        # ti = datetime.datetime.now()
        # ti1 = ti.strftime("%Y-%m-%d %H:%M:%S")
        # # ti = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        # info = (tid, equipName, monitor_id, ti1, '0')
        formatData = []
        for temp in data:
            ti = ti + datetime.timedelta(minutes=30)
            ti2 = ti.strftime("%Y-%m-%d %H:%M:%S")
            info = (tid, equipName, monitor_id, ti2, '0')
            formatData.append(info + tuple(temp.tolist()))
        rowcount = self.mysql1.BatchInsertOperation(sql, formatData)
        if rowcount == 0:
            print("插入失败")
    #
    # def insertVariable(self, tid, equipName, monitor_id, data, ti):
    #     sql = "insert into test_subcable_predict (equip_id,\
    #             `equip_name`,\
    #             `monitor_id`,\
    #             `acquisition_time`,\
    #             `is_deleted`,\
    #             `voltage`,\
    #             `current`,\
    #             `apparent_power`,\
    #             `temperature_opti`,\
    #             `temperature_cablecore`,\
    #             `disturbance_energy`,\
    #             `score`,\
    #             `rank`)\
    #             values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    #     # ti = datetime.datetime.now()
    #     # ti1 = ti.strftime("%Y-%m-%d %H:%M:%S")
    #     # # ti = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    #     # info = (tid, equipName, monitor_id, ti1, '0')
    #     formatData = []
    #     for temp in data:
    #         ti = ti + datetime.timedelta(minutes=30)
    #         ti2 = ti.strftime("%Y-%m-%d %H:%M:%S")
    #         info = (tid, equipName, monitor_id, ti2, '0')
    #         formatData.append(info + tuple(temp.tolist()))
    #     rowcount = self.mysql1.BatchInsertOperation(sql, formatData)
    #     if rowcount == 0:
    #         print("插入失败")

    # 定义插入模型图的参数
    def insertModelParam(self, equip_id, equip_name, monitor_id, dbNumber, paramNumber, data):
        para = "update test_predict_sc_model_" + str(dbNumber) + " set "
        for j in range(1, paramNumber + 1):
            para += "parameter" + str(j) + " =%s,"
        sql = para[:-1] + " where equip_id = %s and monitor_id = %s"
        affectedRows = self.mysql1.updateOperation(sql, tuple(data) + (equip_id, monitor_id))
        if affectedRows > 0:  # 海缆数据存在，更新成功。
            return
        else:  # 海缆记录不存在，插入数据
            para = "insert into test_predict_sc_model_" + str(dbNumber) + " (equip_id,equip_name,monitor_id,"
            s = " values (%s,%s,%s,"
            for i in range(1, paramNumber + 1):
                para += "parameter" + str(i) + ","
                s += "%s,"
            sql = para[:-1] + ")" + s[:-1] + ")"
            self.mysql1.insertOperation(sql, (equip_id, equip_name, monitor_id) + tuple(data))

    def closeConnection(self):
        self.mysql1.closeConnection()


if __name__ == '__main__':
    dao = DAO()
    dao.queryMaxData("CEPD_HL_A")
