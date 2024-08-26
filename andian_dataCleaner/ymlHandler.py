import yaml
'''
    配置文件读取、写入封装
'''


class YamlHandler:
    def __init__(self, file):
        """
        :param file: yamal文件路径
        """
        self.file = file

    #   读取yaml数据
    def read_yaml_data(self):
        with open(self.file, encoding='utf-8') as f:
            data = yaml.load(f.read(), Loader=yaml.FullLoader)
        return data

    # 写入yaml数据
    def write_yaml_data(self, key, value):
        """

        :param key: key
        :param value: 写入的值
        :return:
        """
        with open(self.file, 'r', encoding="utf-8") as f:
            doc = yaml.safe_load(f)

        doc[key] = value

        with open(self.file, 'w', encoding="utf-8") as f:
            yaml.safe_dump(doc,
                           f,
                           default_flow_style=False,
                           allow_unicode=True)
