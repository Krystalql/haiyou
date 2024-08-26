# 导入pymysql模块
import pymysql
import traceback

class MYSQL:
    # 初始化函数，初始化连接列表
    def __init__(self, host,port, user, pwd, dbname):
        self.host = host
        self.port = port
        self.user = user
        self.pwd = pwd
        self.dbname = dbname

    # 建立数据库连接
    def getConnection(self):
        self.conn = pymysql.connect(host=self.host,port = self.port, user=self.user, passwd=self.pwd, db=self.dbname)
        #host(str): MySQL服务器地址
        #port(int): MySQL服务器端口号
        #user(str): 用户名
        #passwd(str): 密码
        #db(str): 数据库名称
        #charset(str): 连接编码

    # 关闭数据库连接
    def closeConnection(self):
        self.conn.close()

    # 获取数据库游标对象cursor,游标对象：用于执行查询和获取结果
    def getCursor(self):

        # 创建游标对象
        cur = self.conn.cursor()
        # 返回
        return cur

    # 查询操作
    def queryOperation(self, sql, data):
        # 建立连接获取游标对象
        cur = self.getCursor()
        # 执行SQL语句
        cur.execute(sql,data)
        # 获取数据的行数
        row = cur.rowcount
        # 获取查询数据
        # fetch*
        # all 所有数据,one 取结果的一行，many(size),去size行
        dataList = cur.fetchall()
        # 关闭游标对象
        cur.close()
        # 返回查询的数据
        return dataList, row

    # 删除操作
    def deleteOperation(self, sql, data):
        # 获取游标对象
        cur = self.getCursor()
        result = 0
        try:
            # 执行SQL语句
            result = cur.execute(sql, data)
            # 正常结束事务
            self.conn.commit()
        except Exception as e:
            # print(e)
            traceback.print_exc()
            # 数据库回滚
            self.conn.rollback()
        # 关闭游标对象
        cur.close()
        return result

    # 数据更新
    def updateOperation(self, sql, data):
        cur = self.getCursor()
        result = 0
        try:
            result = cur.execute(sql, data)
            self.conn.commit()
        except Exception as e:
            # print(e)
            traceback.print_exc()
            self.conn.rollback()
        cur.close()
        return result

    # 添加数据
    # data是元组
    def insertOperation(self, sql, data):
        cur = self.getCursor()
        result = 0
        try:
            result = cur.execute(sql, data)
            self.conn.commit()
        except Exception as e:
            # print(e)
            traceback.print_exc()
            self.conn.rollback()
        cur.close()
        return result

    # 批量添加数据
    # data是列表，里面是元组
    def BatchInsertOperation(self, sql, data):
        cur = self.getCursor()
        rowcount = 0
        try:
            rowcount = cur.executemany(sql, data)
            self.conn.commit()
        except Exception as e:
            traceback.print_exc()
            self.conn.rollback()
        cur.close()
        return rowcount
