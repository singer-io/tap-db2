FROM python:3.5

RUN \
    apt-get update && \
    apt-get install -y unixodbc unixodbc-dev

COPY ibm /opt/ibm

RUN \
    ln -s /opt/ibm/iSeriesAccess/lib64/libcwbcore.so /usr/lib/libcwbcore.so && \
    ln -s /opt/ibm/iSeriesAccess/lib64/libcwbodbc.so /usr/lib/libcwbodbc.so && \
    ln -s /opt/ibm/iSeriesAccess/lib64/libcwbodbcs.so /usr/lib/libcwbodbcs.so && \
    ln -s /opt/ibm/iSeriesAccess/lib64/libcwbrc.so /usr/lib/libcwbrc.so && \
    ln -s /opt/ibm/iSeriesAccess/lib64/libcwbxda.so /usr/lib/libcwbxda.so && \
    ln -s /opt/ibm/lib/libdb2clixml4c.so /usr/lib/libdb2clixml4c.so && \
    ln -s /opt/ibm/lib/libdb2clixml4c.so.1 /usr/lib/libdb2clixml4c.so.1 && \
    ln -s /opt/ibm/lib/libdb2o.so /usr/lib/libdb2o.so && \
    ln -s /opt/ibm/lib/libdb2o.so.1 /usr/lib/libdb2o.so.1 && \
    ln -s /opt/ibm/lib/libdb2.so /usr/lib/libdb2.so && \
    ln -s /opt/ibm/lib/libdb2.so.1 /usr/lib/libdb2.so.1 && \
    ln -s /opt/ibm/lib/libDB2xml4c.so /usr/lib/libDB2xml4c.so && \
    ln -s /opt/ibm/lib/libDB2xml4c.so.58 /usr/lib/libDB2xml4c.so.58 && \
    ln -s /opt/ibm/lib/libDB2xml4c.so.58.0 /usr/lib/libDB2xml4c.so.58.0

COPY docker/odbcinst.ini /etc/odbcinst.ini

ENV PYTHONPATH /opt/code/tap-db2
RUN mkdir -p /opt/code/tap-db2
COPY ./setup.py /opt/code/tap-db2/setup.py
COPY ./tap_db2 /opt/code/tap-db2/tap_db2
RUN pip install /opt/code/tap-db2

WORKDIR /opt/code/tap-db2
ENTRYPOINT ["python", "-m", "tap_db2"]
CMD []
