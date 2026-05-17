import json
import sys
import os
import logging
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, avg, desc, row_number, regexp_replace, when, to_date, date_format, from_unixtime, lower
from pyspark.sql.types import DecimalType
from pyspark.sql.window import Window
from great_expectations.dataset.sparkdf_dataset import SparkDFDataset
from great_expectations.render.renderer import ValidationResultsPageRenderer
from great_expectations.render.view import DefaultJinjaPageView
from datetime import datetime

# Configuração do Logger para rastreabilidade no Airflow
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    stream=sys.stdout
)
logger = logging.getLogger("PipelineLocaliza")

def validate_data(df: DataFrame, suite_path: str = "/opt/airflow/gx/expectations/suite_desafio.json") -> bool:
    """
    Executa a validação de qualidade de dados sob o paradigma Metadata-Driven.

    A função consome um arquivo JSON de expectativas (contrato de dados) e o aplica 
    linha a linha sobre o DataFrame bruto recebido. Ao final, renderiza um relatório 
    HTML estático documentando de forma interativa o percentual de conformidade, 
    volumetria de nulos e anomalias estruturais encontradas na fonte.

    Parâmetros:
        df (DataFrame): DataFrame Spark com os dados brutos da camada Bronze/Raw.
        suite_path (str): Caminho absoluto do arquivo JSON com as regras do Great Expectations.

    Retorna:
        bool: True se o DataFrame passar em todas as regras críticas do contrato; 
              False caso contrário (anomalias detectadas).
    """
    try:
        with open(suite_path, 'r') as file:
            suite = json.load(file)
    except FileNotFoundError:
        logger.error(f"Arquivo de metadados não encontrado em {suite_path}")
        return False
    
    ge_df = SparkDFDataset(df)
    nome_execucao = datetime.now().strftime("desafio_localiza_%Y%m%d_%H%M%S")
    
    for expectation in suite.get("expectations", []):
        func = getattr(ge_df, expectation["expectation_type"])
        func(**expectation["kwargs"])
    
    validation_result = ge_df.validate(run_name=nome_execucao)
    
    document_model = ValidationResultsPageRenderer().render(validation_result)
    html = DefaultJinjaPageView().render(document_model)
    
    output_html_path = "/opt/airflow/gx/uncommitted/data_quality_report.html"
    with open(output_html_path, "w") as html_file:
        html_file.write(html)
    
    logger.info(f"Relatório de Data Quality renderizado em: {output_html_path}")
    return validation_result["success"]

def save_as_parquet(df: DataFrame, path: str) -> None:
    """
    Persiste o DataFrame em disco utilizando o formato de armazenamento Parquet.

    Aplica a operação de coalesce(1) antes da escrita para consolidar os dados em 
    um único arquivo físico, mitigando o problema de Small Files em ambientes locais. 
    A escrita adota o modo 'overwrite' para garantir a idempotência do pipeline em 
    cenários de reprocessamento automáticos pelo orquestrador.

    Parâmetros:
        df (DataFrame): O DataFrame a ser persistido fisicamente.
        path (str): O caminho do diretório de destino no sistema de arquivos.
    """
    logger.info(f"Persistindo arquivo Parquet (Overwrite/Coalesce=1) em: {path}")
    df.coalesce(1).write.mode("overwrite").parquet(path)

def transform_raw_to_silver(df_raw: DataFrame) -> DataFrame:
    """
    Executa a higienização estrutural, saneamento numérico e tipagem dos dados brutos.

    Esta função atua na transição para a camada Silver, aplicando as seguintes regras:
      1. Correção padrão do 'ip_prefix', substituindo separadores inválidos (vírgula por ponto).
      2. Tratamento rigoroso de valores nulos ou strings espúrias (ex: 'none') nas colunas 
         'amount' e 'risk_score', convertendo-as para '0' antes do cast numérico.
      3. Tipagem de precisão matemática fixa via DecimalType(38,4) para resguardar agrupamentos.
      4. Desacoplamento semântico do Unix Timestamp em duas novas dimensões temporais 
         ('data_transacao' e 'hora_transacao') para viabilizar estratégias de particionamento e BI.

    Parâmetros:
        df_raw (DataFrame): DataFrame original contendo os tipos inferidos da fonte CSV.

    Retorna:
        DataFrame: DataFrame modificado e higienizado pronto para consumo analítico.
    """
    df_clean = df_raw.withColumn("ip_prefix", regexp_replace(col("ip_prefix"), ",", "."))
    
    for coluna in ["amount", "risk_score"]:
        df_clean = df_clean.withColumn(
            coluna,
            when(col(coluna).isNull() | (lower(col(coluna)) == "none"), "0")
            .otherwise(col(coluna))
            .cast(DecimalType(38, 4))
        )

    df_clean = df_clean.withColumn("data_transacao", to_date(from_unixtime(col("timestamp")))) \
                       .withColumn("hora_transacao", date_format(from_unixtime(col("timestamp")), "HH:mm:ss"))
    
    return df_clean

def create_gold_table1(df_clean: DataFrame) -> None:
    """
    Gera a Tabela-Resultado 1 da camada Gold (Métrica de Risco por Região).

    Agrupa o conjunto de dados higienizados pela coluna 'location_region', calcula a 
    média aritmética ponderada da coluna 'risk_score' e ordena o resultado final em 
    ordem decrescente. Aplica uma transformação visual tardia (string com padrão de 
    vírgula para o relatório) imediatamente antes de salvar os dados físicos em formato Parquet.

    Parâmetros:
        df_clean (DataFrame): DataFrame oriundo da camada Silver física do disco.
    """
    df_tabela1 = df_clean.groupBy("location_region") \
        .agg(avg("risk_score").alias("media_risk_score")) \
        .orderBy(desc("media_risk_score"))
    
    df_tabela1_output = df_tabela1.withColumn(
        "media_risk_score", 
        regexp_replace(col("media_risk_score").cast(DecimalType(38,4)).cast("string"), "\\.", ",")
    )
    
    save_as_parquet(df_tabela1_output, "/opt/airflow/data/output/gold/tabela1_risk_score")

def create_gold_table2(df_clean: DataFrame) -> None:
    """
    Gera a Tabela-Resultado 2 da camada Gold (Top Ranking de Vendas Recentes).

    Aplica a seguinte esteira lógica otimizada:
      1. Pushdown Filter: Filtra antecipadamente as transações do tipo 'sale' para diminuir 
         drasticamente o volume de dados submetidos ao shuffle na memória do Spark.
      2. Window Function: Particiona os registros por 'receiving_address' e ordena por tempo 
         decrescente para isolar estritamente o evento mais recente (rn == 1) de cada endereço.
      3. Ranking Global: Ordena o conjunto reduzido de forma decrescente pelo valor financeiro 
         ('amount') e limita a saída às 3 principais ocorrências do ecossistema.

    Aplica a formatação visual de exibição da moeda antes de persistir o resultado analítico.

    Parâmetros:
        df_clean (DataFrame): DataFrame oriundo da camada Silver física do disco.
    """
    df_sales = df_clean.filter(col("transaction_type") == "sale")
    
    window_spec = Window.partitionBy("receiving_address").orderBy(desc("data_transacao"), desc("hora_transacao"))
    
    df_tabela2 = df_sales.withColumn("rn", row_number().over(window_spec)) \
        .filter(col("rn") == 1) \
        .orderBy(desc("amount")) \
        .select("receiving_address", "amount","data_transacao", "hora_transacao") \
        .limit(3)
    
    df_tabela2_output = df_tabela2.withColumn(
        "amount", 
        regexp_replace(col("amount").cast("string"), "\\.", ",")
    )
    
    save_as_parquet(df_tabela2_output, "/opt/airflow/data/output/gold/tabela2_top3_sales")

def main() -> None:
    
    caminho_input = "/opt/airflow/data/input/df_fraud_credit.csv"
    
    # 2. Validação Defensiva (Fail-Fast): Checa a existência do arquivo local no container
    if not os.path.exists(caminho_input):
        logger.critical(
            f"❌ CONTRATO DE INFRAESTRUTURA VIOLADO: O arquivo de input não foi encontrado em: {caminho_input}. "
            "Certifique-se de que o arquivo físico foi baixado (caso esteja no .gitignore) "
            "e que os volumes estão mapeados corretamente no docker-compose.yaml."
        )
        # Encerra o script com código de erro 1, fazendo a task do Airflow falhar imediatamente
        sys.exit(1)
    
    logger.info("Inicializando SparkSession...")
    spark = SparkSession.builder \
        .appName("PipelineLocaliza_Desafio") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
   
    logger.info("1. Iniciando ingestão de dados da camada Raw...")
    df = spark.read.csv(caminho_input, header=True, inferSchema=True)
    
    logger.info("2. Executando validação de Data Quality...")
    is_valid = validate_data(df)
    if not is_valid:
        logger.critical("Anomalias detectadas em contrato de dados. Verifique o relatório GX.")
    
    logger.info("3. Aplicando transformações lógicas da camada Silver...")
    df_clean_logico = transform_raw_to_silver(df)
    save_as_parquet(df_clean_logico, "/opt/airflow/data/output/silver")
    
    logger.info("=== QUEBRA DE LINHAGEM (OTIMIZAÇÃO DE PERFORMANCE) ===")
    logger.info("Lendo Silver física do disco para evitar reprocessamento de transformações...")
    df_silver_fisico = spark.read.parquet("/opt/airflow/data/output/silver")
    
    logger.info("4. Processando Tabela-Resultado 1 (Média de Risk Score) na camada Gold...")
    create_gold_table1(df_silver_fisico)
    
    logger.info("5. Processando Tabela-Resultado 2 (Top 3 Vendas Recentes) na camada Gold...")
    create_gold_table2(df_silver_fisico)
    
    logger.info("Pipeline executado integralmente com sucesso. Encerrando SparkSession.")
    spark.stop()

if __name__ == "__main__":
    main()