FROM ubuntu
MAINTAINER Stefan Majer <stefan.majer [at] gmail.com>

ADD https://s3.amazonaws.com/influxdb/influxdb_0.9.4.2_amd64.deb /influxdb_latest_amd64.deb
ADD https://github.com/jiafu1115/influxdb-java/blob/patch-17/influxdb.conf /influxdb.conf
RUN dpkg -i /influxdb_latest_amd64.deb

EXPOSE 8083 8086 8089

CMD ["/opt/influxdb/influxd -config /influxdb.conf"]
