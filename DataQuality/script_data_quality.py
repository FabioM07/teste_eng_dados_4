import argparse
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


DEFAULT_SILVER_PATH = "s3://bucket-silver/tb_cliente"


def parse_args():
    parser = argparse.ArgumentParser(description="Data Quality - tabela Silver de clientes")

    parser.add_argument(
        "--local",
        action="store_true",
        help="Executa localmente utilizando um diretório informado."
    )

    parser.add_argument(
        "--silver-path",
        default=None,
        help="Caminho da tabela Silver em Parquet."
    )

    return parser.parse_args()


def get_spark_session():
    return (
        SparkSession.builder
        .appName("data-quality-clientes-silver")
        .getOrCreate()
    )


def resolve_silver_path(args):
    if args.silver_path:
        return args.silver_path

    return os.getenv("SILVER_PATH", DEFAULT_SILVER_PATH)


def count_condition(df, condition):
    return df.filter(condition).count()


def create_result(rule_name, quantity):
    return {
        "regra": rule_name,
        "status": "PASS" if quantity == 0 else "FAIL",
        "quantidade_registros": quantity
    }


def run_quality_checks(df):
    phone_pattern = r"^\(\d{2}\)\d{5}-\d{4}$"

    results = []

    results.append(create_result(
        "cod_cliente nulo",
        count_condition(df, F.col("cod_cliente").isNull())
    ))

    results.append(create_result(
        "nm_cliente nulo ou vazio",
        count_condition(df, F.col("nm_cliente").isNull() | (F.trim(F.col("nm_cliente")) == ""))
    ))

    results.append(create_result(
        "dt_nascimento_cliente nula",
        count_condition(df, F.col("dt_nascimento_cliente").isNull())
    ))

    results.append(create_result(
        "dt_atualizacao nula",
        count_condition(df, F.col("dt_atualizacao").isNull())
    ))

    results.append(create_result(
        "tp_pessoa nulo ou fora do domínio esperado",
        count_condition(
            df,
            F.col("tp_pessoa").isNull() | (~F.col("tp_pessoa").isin("PF", "PJ"))
        )
    ))

    results.append(create_result(
        "vl_renda nula ou negativa",
        count_condition(df, F.col("vl_renda").isNull() | (F.col("vl_renda") < 0))
    ))

    results.append(create_result(
        "telefone fora do padrão esperado",
        count_condition(
            df,
            F.col("num_telefone_cliente").isNotNull()
            & (~F.col("num_telefone_cliente").rlike(phone_pattern))
        )
    ))

    results.append(create_result(
        "dt_nascimento_cliente futura",
        count_condition(df, F.col("dt_nascimento_cliente") > F.current_date())
    ))

    results.append(create_result(
        "dt_atualizacao futura",
        count_condition(df, F.col("dt_atualizacao") > F.current_date())
    ))

    duplicated_clients = (
        df.groupBy("cod_cliente")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    results.append(create_result(
        "cod_cliente duplicado na camada Silver",
        duplicated_clients
    ))

    return results


def main():
    args = parse_args()
    silver_path = resolve_silver_path(args)

    spark = get_spark_session()

    df_silver = spark.read.parquet(silver_path)

    results = run_quality_checks(df_silver)

    df_results = spark.createDataFrame(results)

    df_results.show(truncate=False)

    failed_checks = df_results.filter(F.col("status") == "FAIL").count()

    if failed_checks > 0:
        raise Exception(f"Data Quality finalizado com falhas: {failed_checks} regra(s) violada(s).")

    print("Data Quality finalizado com sucesso. Todas as regras foram aprovadas.")

    spark.stop()


if __name__ == "__main__":
    main()