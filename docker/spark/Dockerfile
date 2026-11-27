FROM apache/spark:3.5.3-python3

USER root

RUN python3 -m pip install --no-cache-dir \
    numpy==1.24.4 \
    pandas==2.0.3 \
    scipy==1.10.1 \
    pyarrow==12.0.1

RUN mkdir -p /home/spark/.ivy2/cache \
             /home/spark/.ivy2/jars \
    && chown -R spark /home/spark/.ivy2 \
    && chmod -R 755 /home/spark/.ivy2

USER spark