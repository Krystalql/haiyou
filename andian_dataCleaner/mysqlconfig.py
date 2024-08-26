import yaml


class MysqlConfigTrans:
    def __init__(self):
        f = open("ip.yml", 'r')
        ipYml = yaml.load(f, yaml.Loader)

        # destination mysql
        self.destHOST = ipYml['HOST']
        self.destDATABASE = ipYml['DATABASE']
        self.destUSER = ipYml['USER']
        self.destPASSWORD = ipYml['PASSWORD']
        self.destPORT = ipYml['PORT']
        # self.destSelectOld = "select * from (SELECT * FROM andian_transformer_monitor ORDER BY acquisition_time DESC LIMIT 1000) subtab GROUP BY equip_id LIMIT 1000"
        self.destSelectOld = "SELECT * FROM andian_transformer_monitor LIMIT 1000"

        # source mysql
        self.srcHOST = ipYml['HOST']
        self.srcDATABASE = ipYml['DATABASE']
        self.srcUSER = ipYml['USER']
        self.srcPASSWORD = ipYml['PASSWORD']
        self.srcPORT = ipYml['PORT']

    def srcSelectEquipId(self):
        return "SELECT * FROM eem_electrical_equipment_account WHERE equip_type_id=%s"

    def srcSelectMonitorNewById(self):
        return "SELECT * FROM eem_transformer_oil_monitor WHERE equip_id=%s ORDER BY acquisition_time DESC LIMIT 1"

    def srcSelectCurrentNewById(self):
        return f"SELECT * FROM eem_transformer_core_ground_current_input WHERE equip_id=%s ORDER BY acquisition_time DESC LIMIT 1"

    def srcSelectOilNewById(self):
        return f"SELECT * FROM eem_transformer_oil_chromatography_input WHERE equip_id=%s ORDER BY acquisition_time DESC LIMIT 1"

    def srcSelectTemperatureNewById(self):
        return f"SELECT * FROM eem_transformer_oil_temperature_input WHERE equip_id=%s ORDER BY acquisition_time DESC LIMIT 1"

    def srcSelectScoreNewById(self):
        return f"SELECT * FROM eem_equip_common_analysis WHERE equip_id=%s ORDER BY ANALYSIS_TIME DESC LIMIT 1"

    def destInsertNew(self, data):
        fix = ["%s," * len(data)]
        tp_fix = f"({fix[0][:-1]})"
        return "insert into andian_transformer_monitor values" + tp_fix

    def destInsertNewAverage(self, data):
        fix = ["%s," * len(data)]
        tp_fix = f"({fix[0][:-1]})"
        return "insert into andian_transformer_monitor_average values" + tp_fix


class MysqlConfigGis:
    def __init__(self):
        f = open("ip.yml", 'r')
        ipYml = yaml.load(f, yaml.Loader)
        # destination mysql
        self.destHOST = ipYml['HOST']
        self.destDATABASE = ipYml['DATABASE']
        self.destUSER = ipYml['USER']
        self.destPASSWORD = ipYml['PASSWORD']
        self.destPORT = ipYml['PORT']
        self.destSelectOld = "select * from andian_gis_monitor LIMIT 1000"

        # source mysql
        self.srcHOST = ipYml['HOST']
        self.srcDATABASE = ipYml['DATABASE']
        self.srcUSER = ipYml['USER']
        self.srcPASSWORD = ipYml['PASSWORD']
        self.srcPORT = ipYml['PORT']

    def srcSelectEquipId(self):
        return "SELECT * FROM eem_electrical_equipment_account WHERE equip_type_id=%s"

    def srcSelectEquipSectorInfo(self):
        return "SELECT equip_id, equip_name FROM gis_monitor_part"

    def srcSelectPdNewById(self):
        return "SELECT * FROM gis_pd_input WHERE equip_id=%s ORDER BY input_time DESC LIMIT 1"

    def srcSelectSfNewById(self):
        return f"SELECT * FROM gis_sf6_input WHERE equip_id=%s ORDER BY input_time DESC LIMIT 1"

    def srcSelectScoreNewById(self):
        return f"SELECT * FROM eem_equip_common_analysis WHERE equip_id=%s ORDER BY ANALYSIS_TIME DESC LIMIT 1"

    def destInsertNew(self, data):
        fix = ["%s," * len(data)]
        tp_fix = f"({fix[0][:-1]})"
        return "insert into andian_gis_monitor values" + tp_fix


class MysqlConfigSubcable:
    def __init__(self):
        f = open("ip.yml", 'r')
        ipYml = yaml.load(f, yaml.Loader)
        # destination mysql
        self.destHOST = ipYml['HOST']
        self.destDATABASE = ipYml['DATABASE']
        self.destUSER = ipYml['USER']
        self.destPASSWORD = ipYml['PASSWORD']
        self.destPORT = ipYml['PORT']
        self.destSelectOld = "select * from test_subcable_monitor LIMIT 1000"

        # source mysql
        self.srcHOST = ipYml['HOST']
        self.srcDATABASE = ipYml['DATABASE']
        self.srcUSER = ipYml['USER']
        self.srcPASSWORD = ipYml['PASSWORD']
        self.srcPORT = ipYml['PORT']

    def srcSelectMonitorNewById(self):
        return "SELECT * FROM eem_subcable_monitor WHERE equip_id=%s ORDER BY MONITORING_TIME DESC LIMIT 1"

    def srcSelectEquipId(self):
        return "SELECT * FROM eem_electrical_equipment_account WHERE equip_type_id=%s"

    def srcSelectTemperatureNewById(self):
        return f"SELECT * FROM eem_subcable_temperature_input WHERE equip_id=%s ORDER BY INPUT_TIME DESC LIMIT 1"

    def srcSelectDisturbanceNewById(self):
        return f"SELECT * FROM eem_subcable_disturbance_intput WHERE equip_id=%s ORDER BY INPUT_TIME DESC LIMIT 1"

    def srcSelectScoreNewById(self):
        return f"SELECT * FROM eem_equip_common_analysis WHERE equip_id=%s ORDER BY ANALYSIS_TIME DESC LIMIT 1"

    def destInsertNew(self, data):
        fix = ["%s," * len(data)]
        tp_fix = f"({fix[0][:-1]})"
        return "insert into test_subcable_monitor values" + tp_fix

    def destInsertNewAverage(self, data):
        fix = ["%s," * len(data)]
        tp_fix = f"({fix[0][:-1]})"
        return "insert into test_subcable_monitor_average values" + tp_fix
