
FROM bde2020/hadoop-base:2.0.0-hadoop3.2.1-java8

USER root

ARG PIG_VERSION=0.17.0


RUN sed -i 's|deb.debian.org/debian|archive.debian.org/debian|g' /etc/apt/sources.list && \
    sed -i 's|security.debian.org/debian-security|archive.debian.org/debian-security|g' /etc/apt/sources.list && \
    sed -i '/stretch-updates/d' /etc/apt/sources.list && \
    apt-get -o Acquire::Check-Valid-Until=false update && \
    apt-get install -y --no-install-recommends \
        curl ca-certificates tar bash && \
    rm -rf /var/lib/apt/lists/*

RUN curl -fSL \
    "https://archive.apache.org/dist/pig/pig-${PIG_VERSION}/pig-${PIG_VERSION}.tar.gz" \
    -o /tmp/pig.tar.gz && \
    tar -xzf /tmp/pig.tar.gz -C /opt && \
    mv /opt/pig-${PIG_VERSION} /opt/pig && \
    rm /tmp/pig.tar.gz

ENV HADOOP_HOME=/opt/hadoop-3.2.1
ENV HADOOP_CONF_DIR=/etc/hadoop
ENV PIG_HOME=/opt/pig
ENV PATH="${PIG_HOME}/bin:${HADOOP_HOME}/bin:${PATH}"

WORKDIR /workspace

CMD ["bash"]
