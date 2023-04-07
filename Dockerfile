FROM public.ecr.aws/dataminded/dbt:v1.4.0-1

WORKDIR /app
COPY . .

RUN pip3 install --requirement requirements.txt

WORKDIR /app/dbt/dbt_duckdb_tpcds

# install dependencies
RUN dbt deps
