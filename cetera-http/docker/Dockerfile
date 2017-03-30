FROM socrata/java8

ENV CETERA_ROOT /srv/cetera/
ENV CETERA_ARTIFACT cetera-http-assembly.jar
ENV CETERA_CONFIG /etc/cetera.conf
ENV LOG_LEVEL_CETERA_SPECIFIC INFO

RUN mkdir -p $CETERA_ROOT

COPY $CETERA_ARTIFACT $CETERA_ROOT
COPY cetera.conf.j2 /etc/
COPY ship.d /etc/ship.d

EXPOSE 5704
EXPOSE 5705
