
FROM oraclelinux:7-slim
RUN curl -o /etc/yum.repos.d/public-yum-ol7.repo https://yum.oracle.com/public-yum-ol7.repo && \
    yum -y install libaio && \
    yum-config-manager --enable ol7_oracle_instantclient && \
    #yum -y install oracle-instantclient18.3-basic oracle-instantclient18.3-devel oracle-instantclient18.3-sqlplus && \
    yum -y install oracle-instantclient18.3-basic && \
    rm -rf /var/cache/yum

RUN export LD_LIBRARY_PATH=/opt/oracle/instantclient_18_3:$LD_LIBRARY_PATH
RUN export PATH=$PATH:/usr/lib/oracle/18.3/client64/bin  

RUN  ln -s  /usr/lib/oracle/18.3/client64/lib/libclntsh.so.18.1 libclntsh.so

FROM python:3.7-slim

RUN pip install cx_Oracle
RUN pip install --upgrade snowflake-connector-python
RUN pip install pyyaml
RUN pip install pandas
RUN pip install filesplit

RUN mkdir src
RUN mkdir configs

COPY ./src src
COPY ./configs configs

ENTRYPOINT ["python", "src/appMain.py"]