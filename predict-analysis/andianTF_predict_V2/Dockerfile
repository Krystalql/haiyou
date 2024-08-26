FROM andian-base:1.2.0
WORKDIR ./andian_predict_gis
ADD . .
# RUN pip config set global.index-url https://pypi.tuna.tsinghua.edu.cn/simple
# RUN pip install -r requirements.txt
ENTRYPOINT ["python", "-u", "main.py"]
