import os
import sys
from datetime import date

import pytest
from pyspark.sql import SparkSession

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from ETL.script import transform_silver


@pytest.fixture(scope="session")
def spark():
    spark_session = (
        SparkSession.builder
        .appName("unit-test-transform-silver")
        .master("local[*]")
        .getOrCreate()
    )

    yield spark_session

    spark_session.stop()


def test_transform_silver_keep_latest_update(spark):
    input_data = [
        (1, "FABIO", "(11)99999-9999", date(2024, 1, 1), "PF", 1000.0, "20260629"),
        (1, "FABIO", "(11)98888-8888", date(2024, 2, 1), "PF", 1500.0, "20260629"),
    ]

    columns = [
        "cod_cliente",
        "nm_cliente",
        "num_telefone_cliente",
        "dt_atualizacao",
        "tp_pessoa",
        "vl_renda",
        "anomesdia",
    ]

    df = spark.createDataFrame(input_data, columns)

    result = transform_silver(df).collect()

    assert len(result) == 1
    assert result[0]["cod_cliente"] == 1
    assert result[0]["dt_atualizacao"] == date(2024, 2, 1)
    assert result[0]["num_telefone_cliente"] == "(11)98888-8888"


def test_transform_silver_keep_valid_phone(spark):
    input_data = [
        (2, "CLIENTE TESTE", "(21)91234-5678", date(2024, 1, 1), "PF", 2000.0, "20260629"),
    ]

    columns = [
        "cod_cliente",
        "nm_cliente",
        "num_telefone_cliente",
        "dt_atualizacao",
        "tp_pessoa",
        "vl_renda",
        "anomesdia",
    ]

    df = spark.createDataFrame(input_data, columns)

    result = transform_silver(df).collect()

    assert result[0]["num_telefone_cliente"] == "(21)91234-5678"


def test_transform_silver_invalid_phone_to_null(spark):
    input_data = [
        (3, "CLIENTE ERRO", "11999999999", date(2024, 1, 1), "PJ", 3000.0, "20260629"),
    ]

    columns = [
        "cod_cliente",
        "nm_cliente",
        "num_telefone_cliente",
        "dt_atualizacao",
        "tp_pessoa",
        "vl_renda",
        "anomesdia",
    ]

    df = spark.createDataFrame(input_data, columns)

    result = transform_silver(df).collect()

    assert result[0]["num_telefone_cliente"] is None