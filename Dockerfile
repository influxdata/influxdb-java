FROM ubuntu
MAINTAINER Stefan Majer <stefan.majer [at] gmail.com>

ADD http://s3.amazonaws.com/influxdb/influxdb_latest_amd64.deb /influxdb_latest_amd64.deb
RUN dpkg -i /influxdb_latest_amd64.deb

EXPOSE 8083 8086

CMD ["-config=/opt/influxdb/shared/config.toml", "-reset-root=true"]
ENTRYPOINT ["/usr/bin/influxdb"]
