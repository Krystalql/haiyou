FROM registry.cn-shenzhen.aliyuncs.com/c414/andian-base:1.1.0
WORKDIR ./andian_cause_gis
ADD . .
RUN pip config set global.index-url https://pypi.tuna.tsinghua.edu.cn/simple
RUN pip install -r req.txt
