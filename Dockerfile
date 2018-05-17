FROM reg.docker.alibaba-inc.com/kmonitor/golang

MAINTAINER Shi Li "<shili.sl@alibaba-inc.com>"

# admin, http, udp, cluster, graphite, opentsdb, collectd
EXPOSE 8086

COPY . /go/src/github.com/influxdata/influxdb

RUN cd /go/src/github.com/influxdata/influxdb && \
    make && \
    cp -f ./bin/influxd /go/bin/influxd

RUN mkdir -p /etc/influxd

RUN cp /go/src/github.com/influxdata/influxdb/etc/config.sample.toml /etc/influxd/data.toml