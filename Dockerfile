ARG BASE_CONTAINER=jupyter/all-spark-notebook
FROM $BASE_CONTAINER

USER root

RUN apt-get update && apt-get install gnupg2 -y && \
    echo "deb https://dl.bintray.com/sbt/debian /" | tee -a /etc/apt/sources.list.d/sbt.list && \
    curl -sL "https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x2EE0EA64E40A89B84B2DF73499E82A75642AC823" | apt-key add && \
    apt-get update -y && \
    apt-get install sbt -y && \
    git clone https://github.com/seleythen/openaq-streamer && \
    cd openaq-streamer && \
    sbt assembly && \
    echo "spark.jars ${HOME}/openaq-streamer/target/scala-2.12/openaq-streammer-assembly-0.1.0-SNAPSHOT.jar" > /usr/local/spark/conf/spark-defaults.conf