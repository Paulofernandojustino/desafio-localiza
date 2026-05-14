# Usamos uma versão estável e recente do Airflow
FROM apache/airflow:2.8.1-python3.10

# Trocamos para root para instalar dependências de SO
USER root

# Instalação do OpenJDK 17 (Necessário para o PySpark)
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         openjdk-17-jre-headless \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

# Definimos a variável de ambiente do Java
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# Voltamos para o usuário do Airflow para evitar problemas de permissão
USER airflow

# Copiamos e instalamos as bibliotecas Python
COPY requirements.txt /
RUN pip install --no-cache-dir -r /requirements.txt