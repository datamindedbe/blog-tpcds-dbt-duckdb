FROM mozilla/sbt:8u232_1.4.1 as jarbuilder
WORKDIR /app
RUN git clone https://github.com/datamindedbe/spark-sql-perf.git
RUN cd spark-sql-perf && git pull && sbt +package && cd ..
RUN git clone https://github.com/datamindedbe/eks-spark-benchmark.git
RUN cd eks-spark-benchmark/benchmark && mkdir -p libs &&  cp ../../spark-sql-perf/target/scala-2.12/spark-sql-perf_2.12-0.5.1-SNAPSHOT.jar libs/ && sbt assembly && cd ../..

FROM amazonlinux:2 as builder

ENV TPCDS_KIT_VERSION "master"

RUN yum group install -y "Development Tools" && \
    git clone https://github.com/databricks/tpcds-kit.git -b ${TPCDS_KIT_VERSION} /tmp/tpcds-kit && \
    cd /tmp/tpcds-kit/tools && \
    make OS=LINUX

FROM public.ecr.aws/dataminded/spark-k8s-glue:v3.2.1-hadoop-3.3.1-v3

USER 0
RUN apt-get update && apt-get install -y lzop liblzo2-2
# Copy tpcds-kit and benchmark library
COPY --from=builder /tmp/tpcds-kit/tools /opt/tpcds-kit/tools
COPY --from=jarbuilder /app/eks-spark-benchmark/benchmark/target/scala-2.12/*jar .
