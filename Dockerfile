FROM ubuntu
MAINTAINER Stefan Majer <stefan.majer [at] gmail.com>

ADD http://get.influxdb.org/influxdb_0.9.0-rc31_amd64.deb /influxdb_latest_amd64.deb
RUN dpkg -i /influxdb_latest_amd64.deb

EXPOSE 8083 8086 4444

CMD ["/opt/influxdb/influxd"]
