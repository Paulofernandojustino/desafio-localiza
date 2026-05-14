<!DOCTYPE html>
<html lang="pt-BR">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>README - Desafio Técnico Localiza</title>
    <style>
        body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Helvetica, Arial, sans-serif; line-height: 1.6; color: #333; max-width: 900px; margin: 0 auto; padding: 2rem; background-color: #f4f5f7; }
        .container { background-color: #fff; padding: 2.5rem; border-radius: 8px; box-shadow: 0 4px 12px rgba(0,0,0,0.05); }
        h1, h2, h3 { color: #24292e; border-bottom: 1px solid #eaecef; padding-bottom: 0.3em; margin-top: 1.5em; }
        h1 { font-size: 2em; border-bottom: 2px solid #eaecef; margin-top: 0; }
        h2 { font-size: 1.5em; }
        h3 { font-size: 1.25em; border-bottom: none; }
        code { background-color: #f6f8fa; padding: 0.2em 0.4em; border-radius: 3px; font-family: monospace; font-size: 90%; color: #e83e8c; }
        pre { background-color: #282c34; color: #abb2bf; padding: 16px; overflow: auto; border-radius: 6px; font-family: monospace; line-height: 1.4; }
        pre code { background-color: transparent; padding: 0; color: inherit; }
        ul, ol { padding-left: 2em; }
        li { margin-bottom: 0.5em; }
        .badge { background-color: #0366d6; color: white; padding: 4px 8px; border-radius: 4px; font-size: 0.85em; font-weight: bold; }
    </style>
</head>
<body>
    <div class="container">
        <h1>🚀 Pipeline de Dados - Desafio Técnico Localiza</h1>
        <p>Este repositório apresenta uma solução robusta e escalável para o desafio técnico de Engenharia de Dados. A arquitetura foi concebida sob os pilares de <strong>qualidade de dados automatizada</strong>, <strong>processamento de alto desempenho</strong> e <strong>observabilidade ponta a ponta</strong>.</p>
        
        <h2>🛠️ Stack Tecnológica</h2>
        <ul>
            <li><strong>Motor de Processamento:</strong> Python 3.10 + Apache Spark (PySpark 3.5.0)</li>
            <li><strong>Orquestração:</strong> Apache Airflow 2.8.1 (LocalExecutor)</li>
            <li><strong>Data Quality:</strong> Great Expectations (Arquitetura Metadata-driven)</li>
            <li><strong>Apresentação & Governança:</strong> Streamlit (Dashboard Analítico)</li>
            <li><strong>Infraestrutura:</strong> Docker & Docker Compose</li>
        </ul>

        <h2>🏗️ Decisões Arquiteturais (ADR)</h2>
        <p>Para demonstrar senioridade e minimizar custos operacionais, foram adotadas as seguintes estratégias estruturais:</p>
        <ol>
            <li><strong>Shift-Left Data Quality:</strong> O Great Expectations atua na camada <em>Raw</em>, logo após a ingestão. Validar o dado bruto antes da limpeza permite que o relatório de qualidade reflita a realidade exata da fonte, documentando anomalias (como registros 'phishing' ou tipos incorretos) que seriam mascaradas se a validação fosse pós-saneamento.</li>
            <li><strong>Arquitetura Metadata-Driven:</strong> As regras de validação não estão "hardcoded". Elas residem em arquivos JSON (<code>gx/expectations/</code>), permitindo que as regras sejam dinâmicas sem necessidade de recompilar o código de processamento.</li>
            <li><strong>Processamento Local (Minimização de Overhead):</strong> Utilizou-se o Spark em modo <code>local[*]</code> embutido no container do Airflow. Isso elimina a necessidade de subir nós dedicados (Master/Worker), otimizando severamente o consumo de recursos na máquina avaliadora, o que reflete uma postura direta de <strong>minimização de custos de infraestrutura</strong>.</li>
            <li><strong>Saneamento e Integridade Numérica:</strong>
                <ul>
                    <li><strong>Tipagem Decimal:</strong> Colunas <code>amount</code> e <code>risk_score</code> são tratadas como <code>DecimalType(38,4)</code> durante todo o pipeline para garantir exatidão matemática nas agregações (evitando o erro natural de precisão de <em>floats</em> comuns).</li>
                    <li><strong>Conversão Visual:</strong> A substituição de ponto por vírgula ocorre estritamente no estágio de saída, preservando a integridade das ordenações e agrupamentos no Spark.</li>
                    <li><strong>Timestamp Unix:</strong> Conversão nativa com a função C do Spark (<code>from_unixtime</code>) para otimizar I/O.</li>
                </ul>
            </li>
        </ol>

        <h2>📁 Estrutura do Projeto</h2>
<pre><code>├── dags/                # Definições de DAGs do Airflow
├── src/                 # Script principal de processamento PySpark
├── gx/                  # Metadados e Relatórios do Great Expectations
├── data/
│   ├── input/           # Ingestão (Arquivo CSV bruto)
│   └── output/          # Tabelas finais em Parquet (Resultados)
├── dashboard.py         # Dashboard Streamlit (Frontend/Apresentação)
├── docker-compose.yml   # Orquestração de containers e mapeamento de volumes
├── Dockerfile           # Imagem enxuta (Airflow + OpenJDK 17 + Spark)
└── requirements.txt     # Dependências de processamento python</code></pre>

        <h2>🚀 Como Executar</h2>
        <h3>1. Inicialização</h3>
        <p>Na raiz do projeto, execute o comando abaixo no terminal para construir a imagem otimizada e subir os serviços:</p>
<pre><code>docker-compose up -d --build</code></pre>

        <h3>2. Orquestração (Airflow)</h3>
        <p>Acesse <code>http://localhost:8080</code> (Usuário/Senha: <code>admin</code> / <code>admin</code>).</p>
        <ul>
            <li>Ative a DAG <code>pipeline_localiza_desafio</code> e dispare manualmente (Trigger DAG).</li>
            <li>Acompanhe o processamento em tempo real através da aba <strong>Logs</strong> da task <code>run_pyspark_and_dq</code>. Os logs contornam o buffer padrão do Python via biblioteca <code>logging</code>, garantindo rastreabilidade técnica imersiva.</li>
        </ul>

        <h3>3. Visualização de Resultados (Dashboard)</h3>
        <p>Acesse <code>http://localhost:8081</code> para o portal interativo de resultados.</p>
        <ul>
            <li><strong>Aba Qualidade:</strong> Renderização interativa do <em>Data Docs</em> do Great Expectations (diagnóstico de anomalias da fonte).</li>
            <li><strong>Aba Saída do Teste:</strong> Visualização nativa das tabelas-resultado (Média por Região e Top 3 Sales) geradas no formato Parquet, sem necessidade de ferramentas de terceiros para o RH ou Avaliador ler os arquivos.</li>
        </ul>

        <h2>📊 Destaques de Eficiência no Código</h2>
        <ul>
            <li><span class="badge">Pushdown Filters</span> Aplicados antes das operações de <em>Window Functions</em> para reduzir a pegada de memória durante o <em>shuffle</em> do PySpark.</li>
            <li><span class="badge">Idempotência</span> O pipeline limpa a área de output com `mode("overwrite")` e utiliza `coalesce(1)` para evitar fragmentação de disco em pequenos lotes.</li>
            <li><span class="badge">Docker Healthchecks</span> Prevenção de <em>Crash Loops</em> com monitoramento de serviço de banco (Postgres) diretamente no Compose.</li>
        </ul>
    </div>
</body>
</html>