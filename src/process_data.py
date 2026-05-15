import json
import sys
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, desc, row_number, regexp_replace, when, to_date, date_format, from_unixtime
from pyspark.sql.types import DecimalType
from pyspark.sql.window import Window
from great_expectations.dataset.sparkdf_dataset import SparkDFDataset
from great_expectations.render.renderer import ValidationResultsPageRenderer
from great_expectations.render.view import DefaultJinjaPageView
from datetime import datetime

 # Configurar o logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("PipelineLocaliza")

def validate_data(df, suite_path="/opt/airflow/gx/expectations/suite_desafio.json"):
    """
    Executa a validação de qualidade de dados (Metadata-Driven).
    Gera o relatório HTML estático de anomalias e conformidade.
    """
    try:
        with open(suite_path, 'r') as file:
            suite = json.load(file)
    except FileNotFoundError:
        print(f"Erro: Arquivo de metadados não encontrado em {suite_path}")
        return False
    
    ge_df = SparkDFDataset(df)
    #Gera um identificador único (Ex: desafio_localiza_20260514_194500)
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
    logger.info(f"Relatório de Qualidade gerado em: {output_html_path}")
    print(f"Relatório de Qualidade gerado em: {output_html_path}")
    return validation_result["success"]

def main():
    # Inicializa a sessão do Spark otimizada para uso local
    spark = SparkSession.builder \
        .appName("PipelineLocaliza_Desafio") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    print("><><><><><><><><><><><><><<><><><><><><<><><><><><><<><><><><><><<><><><><><><<><><><><><><")
    print("><><><><><><><><><><><><><<><><><><><><<><><><><><><<><><><><><><<><><><><><><<><><><><><><")
    print("><><><><><><><><><><><><><<><><><><><><<><><><><><><<><><><><><><<><><><><><><<><><><><><><")
    print(">>>>               Pipeline de Processamento Localiza - Desafio Técnico               <<<< ")
    print(">>>>               Pipeline de Processamento Localiza - Desafio Técnico               <<<< ")
    print(">>>>               Pipeline de Processamento Localiza - Desafio Técnico               <<<< ")
    print(">>>>               Pipeline de Processamento Localiza - Desafio Técnico               <<<< ")
    print(">>>>               Pipeline de Processamento Localiza - Desafio Técnico               <<<< ")
    print("><><><><><><><><><><><><><<><><><><><><<><><><><><><<><><><><><><<><><><><><><<><><><><><><")
    print("><><><><><><><><><><><><><<><><><><><><<><><><><><><<><><><><><><<><><><><><><<><><><><><><")
    print("><><><><><><><><><><><><><<><><><><><><<><><><><><><<><><><><><><<><><><><><><<><><><><><><")
    
    logger.info("1. Iniciando ingestão de dados...")
    print("1. Iniciando ingestão de dados...")
    df = spark.read.csv("/opt/airflow/data/input/df_fraud_credit.csv", header=True, inferSchema=True)
    
    logger.info("2. Executando validação de Data Quality na fonte bruta...")
    is_valid = validate_data(df)
    if not is_valid:
        logger.warning("Aviso: Anomalias detectadas e documentadas no relatório GX. Prosseguindo para higienização.")

    logger.info("3. Executando Data Cleaning e Padronização...")
    
    # Tratamento do ip_prefix
    df_clean = df.withColumn("ip_prefix", regexp_replace(col("ip_prefix"), ",", "."))
    
    # Tratamento de nulos/none e cast seguro para as colunas numéricas
    for coluna in ["amount", "risk_score"]:
        df_clean = df_clean.withColumn(
            coluna,
            when(col(coluna).isNull() | (col(coluna) == "none"), "0").otherwise(col(coluna))
        )
        # Garante 4 casas decimais e tipagem numérica para viabilizar cálculos
        df_clean = df_clean.withColumn(coluna, col(coluna).cast(DecimalType(38, 4)))

    # Tratamento do Unix Timestamp (assumindo segundos. Se for milissegundos, use col("timestamp") / 1000)
    df_clean = df_clean.withColumn("data_transacao", to_date(from_unixtime(col("timestamp")))) \
                       .withColumn("hora_transacao", date_format(from_unixtime(col("timestamp")), "HH:mm:ss"))

    logger.info(" SALVANDO TABELA INTERMEDIÁRIA (Silver) para análises exploratórias e auditoria...")
    df_clean.coalesce(1).write.mode("overwrite").parquet("/opt/airflow/data/output/silver/silver_table")
    
    logger.info("4. Processando Tabela-Resultado 1 (Média de Risk Score)...")
    df_tabela1 = df_clean.groupBy("location_region") \
        .agg(avg("risk_score").alias("media_risk_score")) \
        .orderBy(desc("media_risk_score"))
    
    # Formatação visual final: Decimal (38,4) para String com vírgula
    df_tabela1_output = df_tabela1.withColumn(
        "media_risk_score", 
        regexp_replace(col("media_risk_score").cast(DecimalType(38,4)).cast("string"), "\\.", ",")
    )
    
    df_tabela1_output.coalesce(1).write.mode("overwrite").parquet("/opt/airflow/data/output/gold/tabela1_risk_score")
    
    logger.info("5. Processando Tabela-Resultado 2 (Top 3 Recebimentos Recentes)...")
    # Filtro antecipado
    df_sales = df_clean.filter(col("transaction_type") == "sale")
    
    window_spec = Window.partitionBy("receiving_address").orderBy(desc("data_transacao"), desc("hora_transacao"))
    
    df_tabela2 = df_sales.withColumn("rn", row_number().over(window_spec)) \
        .filter(col("rn") == 1) \
        .orderBy(desc("amount")) \
        .select("receiving_address", "amount","data_transacao", "hora_transacao") \
        .limit(3)
    
    # Formatação visual final: Decimal (38,4) para String com vírgula
    df_tabela2_output = df_tabela2.withColumn(
        "amount", 
        regexp_replace(col("amount").cast("string"), "\\.", ",")
    )
    logger.info("6. Salvando tabela 2.")    
    df_tabela2_output.coalesce(1).write.mode("overwrite").parquet("/opt/airflow/data/output/gold/tabela2_top3_sales")
    
    logger.info("Processamento finalizado com sucesso. Tabelas exportadas.")
    print("><><><><><><><><><><><><><<><><><><><><<><><><><><><<><><><><><><<><><><><><><<><><><><><><")
    print("><><><><><><><><><><><><><<><><><><><><<><><><><><><<><><><><><><<><><><><><><<><><><><><><")
    print("><><><><><><><><><><><><><<><><><><><><<><><><><><><<><><><><><><<><><><><><><<><><><><><><")
    print(">>>>               Pipeline de Processamento Localiza - Desafio Técnico               <<<< ")
    print(">>>>               Pipeline de Processamento Localiza - Desafio Técnico               <<<< ")
    print(">>>>               Pipeline de Processamento Localiza - Desafio Técnico               <<<< ")
    print(">>>>               Pipeline de Processamento Localiza - Desafio Técnico               <<<< ")
    print(">>>>               Pipeline de Processamento Localiza - Desafio Técnico               <<<< ")
    print("><><><><><><><><><><><><><<><><><><><><<><><><><><><<><><><><><><<><><><><><><<><><><><><><")
    print("><><><><><><><><><><><><><<><><><><><><<><><><><><><<><><><><><><<><><><><><><<><><><><><><")
    print("><><><><><><><><><><><><><<><><><><><><<><><><><><><<><><><><><><<><><><><><><<><><><><><><")
    spark.stop()

if __name__ == "__main__":
   
    main()