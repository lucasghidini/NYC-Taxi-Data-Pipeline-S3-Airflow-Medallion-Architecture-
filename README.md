# NYC Taxi Data Pipeline: S3 & Airflow (Medallion Architecture)
Este projeto implementa um pipeline de dados automatizado utilizando Apache Airflow e AWS S3 para processar o dataset público da NYC Taxi. 

O pipeline segue a Arquitetura de Medalhão (Bronze, Silver e Gold), garantindo a qualidade e agregação dos dados para análise.

## Tecnologias Utilizadas
- Orquestração: Apache Airflow 2.8.1 (Dockerizado)
- Armazenamento: AWS S3 (via S3Hook e S3KeySensor)
- Processamento: Python & Pandas
- Formato de Dados: Parquet (Otimizado para leitura colunar)
- Infraestrutura: Docker

## Arquitetura do Pipeline
- O fluxo de dados foi desenhado para ser resiliente e eficiente:
- Bronze (Raw): Um S3KeySensor monitora o bucket amzn-s3-projetos aguardando a chegada dos dados brutos em formato Parquet.
- Silver (Cleaned): Os dados são processados para remover valores nulos em colunas críticas, filtrar viagens com distância zero e calcular a métrica de trip_duration (duração da viagem).
- Gold (Aggregated): Os dados limpos são agregados por payment_type para calcular médias de faturamento, distância e tempo, gerando uma visão pronta para dashboards.
- Notification: O pipeline encerra enviando um alerta de sucesso (via EmailOperator).

## Desafios Técnicos & Soluções

Durante o desenvolvimento, implementei soluções para problemas comuns de engenharia de dados:

Otimização de Memória (OOM): Para lidar com o volume de dados no Docker/Windows, configurei o .wslconfig para expandir os limites de RAM do WSL2 e utilizei seleção de colunas no Pandas (read_parquet), reduzindo drasticamente o uso de memória.

Segurança e Conectividade: Utilização de S3Hook para gerenciar credenciais de forma segura através das Connections do Airflow, evitando chaves expostas no código.

Resiliência: Implementação de sensores para garantir que o processamento só inicie se o dado estiver disponível, evitando falhas desnecessárias e desperdício de recursos.

## Como Rodar este Projeto
Configurar o Docker:
```bash
  docker-compose up -d
```
Configurar Conexões no Airflow:

Crie uma conexão `aws_s3_conn` (Amazon Web Services) com suas chaves.

Crie uma conexão `smtp_default` (Email) para as notificações.

Limites do WSL2 (Se estiver no Windows): Certifique-se de que o arquivo .wslconfig na sua pasta de usuário tenha pelo menos 4GB de RAM alocados.



Lucas Ghdini, 2026
