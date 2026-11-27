
FROM apache/hive:3.1.3

USER root

RUN mkdir -p /opt/hive/conf

# PostgreSQL JDBC driver
RUN rm -f /opt/hive/lib/postgresql-9.4.1208.jre7.jar

ADD https://repo.maven.apache.org/maven2/org/postgresql/postgresql/42.7.5/postgresql-42.7.5.jar /opt/hive/lib/postgresql-42.7.5.jar

RUN chmod 644 /opt/hive/lib/postgresql-42.7.5.jar

# Avoid Hadoop 2.7 and Hadoop 3.1 conflicts
RUN mkdir -p /opt/tez/backup-jars && \
    mv /opt/tez/lib/hadoop-mapreduce-client-common-2.7.0.jar /opt/tez/backup-jars/ && \
    mv /opt/tez/lib/hadoop-mapreduce-client-core-2.7.0.jar /opt/tez/backup-jars/

# Beeline home directory
RUN mkdir -p /home/hive/.beeline && \
    chown -R hive /home/hive

USER hive
