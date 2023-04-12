# Tpcds-dbt-duckdb

This repository contains the tpcds queries inside a standard dbt project, which uses the [dbt-duckdb](https://github.com/jwills/dbt-duckdb) adapter. 

## Prerequisites

### Data 
The data is generated using the [Databricks toolkit](https://github.com/databricks/tpcds-kit) together with the [Databricks sql perf](https://github.com/databricks/spark-sql-perf).
The resulting jars are added to a spark docker container following the instructions provided in [eks spark benchmark](https://github.com/aws-samples/eks-spark-benchmark) and the full setup can be seen in `data/Dockerfile`.

#### Generate data locally
We use dsdgen of the databricks toolkit for generating the data. An example on how to use the resulting docker image:

```shell
docker build -f data/Dockerfile -t tpcds-benchmark .
docker run -v /tmp/tpcds:/var/data -it sql-benchmark /opt/spark/bin/spark-submit --master "local[*]" --name somename \
       --deploy-mode client --class com.amazonaws.eks.tpcds.DataGeneration local:///opt/spark/work-dir/eks-spark-benchmark-assembly-1.0.jar \ 
       /var/data /opt/tpcds-kit/tools parquet 1 10 false false true # These are the application arguments required by the DataGeneration class: data location, path to tpcds toolkit, data format, scale factor, number partitions, create partitioned fact tables, shuffle to get partitions into single files, set logging to WARN 
```

The previous command generates all input data as parquet files with a scale factor of 1 and 10 partitions (For the benchmark we used 100 and 100 as values). If you want to generate more date, you should change the corresponding parameters.
The data is written to `/var/data` in the docker container which is mounted under `/tmp/tpcds`.

#### Generate data on eks
The same Spark container can be used when generating data in eks. If you add a role to the pod, you can directly write data to a s3 path. 

## Execute the code

In order to execute the code locally, you need to install:

- [dbt](https://docs.getdbt.com/dbt-cli/installation/)
- [pyenv](https://github.com/pyenv/pyenv) (recommended)

If you want to run the code remotely, this repository contains a dags folder in order to execute the code on [Conveyor](https://conveyordata.com).
The code in this repository is packaged in a Docker container starting from a dbt base image. The base image is publicly available and the content is described [here](https://docs.conveyordata.com/technical-reference/docker#dbt).

If you want to run the benchmark yourself, this is relatively simple.
The Airflow Dag code (in the `dags` folder) can be altered to run on any Airflow installations, by replacing the `ConveyorContainerOperatorV2` to a [KubernetesPodOperator](https://airflow.apache.org/docs/apache-airflow-providers-cncf-kubernetes/stable/operators.html)

### Queries
I copied all queries from duckdb tpcds benchmark and made them work with s3 input/output.
The original duckdb queries can be found [here](https://github.com/duckdb/duckdb/tree/master/extension/tpcds/dsdgen/queries)

## Tpc-ds results

We ran the benchmark for all queries on m.2xlarge machines, which have 8 vcpu and 32Gb of RAM and attached 100GB of disk storage.
All except 5 queries return successfully. I need to investigate further why these 5 queries go OOM, even on larger instances.

| Query | Time (s) |
|-------|----------|
| q01   | 9.55     |
| q02   | 18.12    |
| q03   | 11.46    |
| q04   | 83.04    |
| q05   | 42.64    |
| q06   | 41.28    |
| q07   | 21.72    |
| q08   | 13.75    |
| q09   | 59.95    |
| q10   | 20.16    |
| q11   | 47.93    |
| q12   | 5.15     |
| q13   | 29.36    |
| q14   | 147.68   |
| q15   | 11.69    |
| q16   | 27.44    |
| q17   | 18.74    |
| q18   | 16.84    |
| q19   | OOM      |
| q20   | 5.05     |
| q21   | 8.73     |
| q22   | 26.91    |
| q23   | OOM      |
| q24   | 25.96    |
| q25   | 21.31    |
| q26   | 10.65    |
| q27   | 42.43    |
| q28   | 45.84    |
| q29   | 15.92    |
| q30   | 10.01    |
| q31   | 31.35    |
| q32   | 9.58     |
| q33   | 21.29    |
| q34   | 7.89     |
| q35   | 13.10    |
| q36   | 30.01    |
| q37   | 12.73    |
| q38   | 15.84    |
| q39   | 14.14    |
| q40   | 8.84     |
| q41   | 1.16     |
| q42   | 7.15     |
| q43   | 8.14     |
| q44   | 20.78    |
| q45   | 6.02     |
| q46   | 13.50    |
| q47   | 53.80    |
| q48   | 13.83    |
| q49   | 35.11    |
| q50   | 11.07    |
| q51   | 43.87    |
| q52   | 7.17     |
| q53   | 8.68     |
| q54   | 67.14    |
| q55   | 6.87     |
| q56   | 19.42    |
| q57   | 27.39    |
| q58   | 23.86    |
| q59   | 29.74    |
| q60   | 18.17    |
| q61   | 22.46    |
| q62   | 4.42     |
| q63   | 8.64     |
| q64   | OOM      |
| q65   | 29.27    |
| q66   | 14.57    |
| q67   | 521.49   |
| q68   | 15.25    |
| q69   | 14.55    |
| q70   | 20.36    |
| q71   | 22.41    |
| q72   | 47.15    |
| q73   | 7.76     |
| q74   | 33.63    |
| q75   | 53.12    |
| q76   | 15.83    |
| q77   | 29.11    |
| q78   | 63.17    |
| q79   | 13.92    |
| q80   | 48.35    |
| q81   | 8.38     |
| q82   | 14.79    |
| q83   | 8.15     |
| q84   | 5.24     |
| q85   | 12.43    |
| q86   | 5.28     |
| q87   | 17.78    |
| q88   | 31.30    |
| q89   | 9.75     |
| q90   | 5.95     |
| q91   | 5.09     |
| q92   | 7.26     |
| q93   | 15.25    |
| q94   | 13.10    |
| q95   | OOM      |
| q96   | 4.88     |
| q97   | 20.31    |
| q98   | 7.73     |
| q99   | 7.05     |
