provider "aws" {
  region = "us-east-1"
}

resource "aws_glue_job" "teste_eng_dados" {
  name     = "teste-eng-dados-clientes"
  role_arn = "arn:aws:iam::123456789012:role/glue-role-teste-eng-dados"

  glue_version      = "5.0"
  number_of_workers = 10
  worker_type       = "G.1X"

  command {
    name            = "glueetl"
    script_location = "s3://bucket-scripts/etl/script.py"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language"                     = "python"
    "--enable-metrics"                   = "true"
    "--enable-continuous-cloudwatch-log" = "true"

    "INPUT_PATH"  = "s3://bucket-input/clientes_sinteticos.csv"
    "BRONZE_PATH" = "s3://bucket-bronze/tabela_cliente_landing"
    "SILVER_PATH" = "s3://bucket-silver/tb_cliente"
  }

  execution_property {
    max_concurrent_runs = 1
  }

  execution_class = "STANDARD"

  tags = {
    projeto = "teste_eng_dados"
  }
}