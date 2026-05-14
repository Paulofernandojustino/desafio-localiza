# desafio-localiza
Teste tecnico para engenharia de dados

```
desafio-localiza/
├── dags/                      # DAGs do Airflow (Orquestração)
│   └── pipeline_vendas.py
├── data/
│   ├── input/                 # Onde o CSV baixado vai ficar
│   └── output/                # Onde as tabelas-resultado serão salvas
├── gx/                        # Diretório do Great Expectations
│   ├── expectations/          # Aqui ficam os JSONs (Metadados das regras)
│   ├── checkpoints/           # Configuração de execução das regras
│   └── uncommitted/           # Onde os Data Docs (HTML) serão gerados
├── src/                       # Scripts PySpark
│   └── process_data.py
├── docker-compose.yml         # Sobe o Airflow, Spark e dependências
├── Dockerfile                 # Imagem customizada com Airflow + PySpark + GE
├── requirements.txt           # Bibliotecas Python (pyspark, great_expectations, apache-airflow)
└── README.md                  # Manual de execução (Obrigatório!)
```