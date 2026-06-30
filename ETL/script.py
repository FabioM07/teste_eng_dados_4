import argparse
import os
from datetime import datetime

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    DateType,
    DoubleType,
)


DEFAULT_INPUT_PATH = "datasets/clientes_sinteticos.csv"
DEFAULT_BRONZE_S3_PATH = "s3://bucket-bronze/tabela_cliente_landing"
DEFAULT_SILVER_S3_PATH = "s3://bucket-silver/tb_cliente"


def parse_args():
    parser = argparse.ArgumentParser(
        description="ETL de clientes - Bronze e Silver"
    )

    parser.add_argument(
        "--local",
        action="store_true",
        help="Executa o pipeline em modo local, gravando em diretórios locais."
    )

    parser.add_argument(
        "--base-dir",
        default="datasets",
        help="Diretório base para execução local."
    )

    parser.add_argument(
        "--input-path",
        default=DEFAULT_INPUT_PATH,
        help="Caminho do arquivo CSV de entrada."
    )

    args, _ = parser.parse_known_args()
    return args


def resolve_paths(args):
    if args.local:
        bronze_path = os.path.join(
            args.base_dir,
            "bronze",
            "tabela_cliente_landing"
        )

        silver_path = os.path.join(
            args.base_dir,
            "silver",
            "tb_cliente"
        )

        os.makedirs(os.path.dirname(bronze_path), exist_ok=True)
        os.makedirs(os.path.dirname(silver_path), exist_ok=True)

        return args.input_path, bronze_path, silver_path

    input_path = os.getenv("INPUT_PATH", args.input_path)
    bronze_path = os.getenv("BRONZE_PATH", DEFAULT_BRONZE_S3_PATH)
    silver_path = os.getenv("SILVER_PATH", DEFAULT_SILVER_S3_PATH)

    return input_path, bronze_path, silver_path


def get_spark_session():
    return (
        SparkSession.builder
        .appName("teste-eng-dados-clientes")
        .getOrCreate()
    )


def get_schema():
    return StructType([
        StructField("cod_cliente", IntegerType(), False),
        StructField("nm_cliente", StringType(), True),
        StructField("nm_pais_cliente", StringType(), True),
        StructField("nm_cidade_cliente", StringType(), True),
        StructField("nm_rua_cliente", StringType(), True),
        StructField("num_casa_cliente", IntegerType(), True),
        StructField("telefone_cliente", StringType(), True),
        StructField("dt_nascimento_cliente", DateType(), True),
        StructField("dt_atualizacao", DateType(), True),
        StructField("tp_pessoa", StringType(), True),
        StructField("vl_renda", DoubleType(), True),
    ])


def read_input_file(spark, input_path):
    return (
        spark.read
        .option("header", True)
        .option("sep", ",")
        .option("dateFormat", "yyyy-MM-dd")
        .schema(get_schema())
        .csv(input_path)
    )


def add_processing_partition(df):
    anomesdia = datetime.today().strftime("%Y%m%d")
    return df.withColumn("anomesdia", F.lit(anomesdia))


def transform_bronze(df):
    return (
        df
        .withColumn("nm_cliente", F.upper(F.col("nm_cliente")))
        .withColumnRenamed("telefone_cliente", "num_telefone_cliente")
    )


def transform_silver(df_bronze):
    phone_pattern = r"^\(\d{2}\)\d{5}-\d{4}$"

    window_cliente = (
        Window
        .partitionBy("cod_cliente")
        .orderBy(F.col("dt_atualizacao").desc())
    )

    return (
        df_bronze
        .withColumn("row_num", F.row_number().over(window_cliente))
        .filter(F.col("row_num") == 1)
        .drop("row_num")
        .withColumn(
            "num_telefone_cliente",
            F.when(
                F.col("num_telefone_cliente").rlike(phone_pattern),
                F.col("num_telefone_cliente")
            ).otherwise(F.lit(None))
        )
    )


def write_partitioned_parquet(df, path):
    (
        df.write
        .mode("overwrite")
        .partitionBy("anomesdia")
        .parquet(path)
    )


def main():
    args = parse_args()

    input_path, bronze_path, silver_path = resolve_paths(args)

    spark = get_spark_session()

    df_input = read_input_file(spark, input_path)

    df_bronze = (
        df_input
        .transform(transform_bronze)
        .transform(add_processing_partition)
    )

    write_partitioned_parquet(df_bronze, bronze_path)

    # Em ambiente AWS Glue/Athena, após a escrita física dos arquivos
    # particionados no S3, também é necessário registrar a partição lógica
    # na tabela do Glue Data Catalog.
    #
    # Exemplo:
    #
    # ALTER TABLE tabela_cliente_landing
    # ADD IF NOT EXISTS PARTITION (anomesdia='20260629')
    # LOCATION 's3://bucket-bronze/tabela_cliente_landing/anomesdia=20260629/';

    df_silver = transform_silver(df_bronze)

    write_partitioned_parquet(df_silver, silver_path)

    # Exemplo de criação da partição lógica para a tabela Silver:
    #
    # ALTER TABLE tb_cliente
    # ADD IF NOT EXISTS PARTITION (anomesdia='20260629')
    # LOCATION 's3://bucket-silver/tb_cliente/anomesdia=20260629/';
    #
    # Como alternativa, também seria possível utilizar MSCK REPAIR TABLE
    # ou Glue Crawler para sincronizar as partições automaticamente.

    print("ETL finalizado com sucesso.")
    print(f"Input: {input_path}")
    print(f"Bronze: {bronze_path}")
    print(f"Silver: {silver_path}")

    spark.stop()


if __name__ == "__main__":
    main()