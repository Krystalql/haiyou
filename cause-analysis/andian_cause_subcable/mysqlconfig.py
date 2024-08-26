import yaml


class MysqlConfig:

    def __init__(self):
        # test mysql
        ymlOpen = open("ip.yml", 'r')
        ymlCode = yaml.load(ymlOpen, Loader=yaml.Loader)

        # test mysql
        self.HOST = ymlCode['HOST']
        self.DATABASE = ymlCode['DATABASE']
        self.USER = ymlCode['USER']
        self.PASSWORD = ymlCode['PASSWORD']
        self.PORT = ymlCode['PORT']
        self.selectMonitorAll = "select * from andian_gis_monitor ORDER BY input_time DESC LIMIT 2000"
        self.selectBenchAll = "SELECT id, equip_id, time, score FROM andian_benchmark_normal_gis ORDER BY time DESC LIMIT 50"
