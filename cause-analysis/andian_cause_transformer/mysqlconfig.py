import yaml


class MysqlConfig:

    def __init__(self):
        ymlOpen = open("ip.yml", 'r')
        ymlCode = yaml.load(ymlOpen, Loader=yaml.Loader)

        # test mysql
        self.HOST = ymlCode['HOST']
        self.DATABASE = ymlCode['DATABASE']
        self.USER = ymlCode['USER']
        self.PASSWORD = ymlCode['PASSWORD']
        self.PORT = ymlCode['PORT']
        self.selectMonitorAll = "select * from andian_transformer_monitor ORDER BY acquisition_time DESC LIMIT 2000"
        self.selectBenchAll = "SELECT id, equip_id, time, score FROM andian_benchmark_normal_transformer ORDER BY " \
                              "time DESC LIMIT 50 "

    def truncateTable(self, tableName):
        return f"TRUNCATE TABLE {tableName}"

    def srcSelectEquipId(self):
        return "SELECT * FROM eem_electrical_equipment_account WHERE equip_type_id=%s"
