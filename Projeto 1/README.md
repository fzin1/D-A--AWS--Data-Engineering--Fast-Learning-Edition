# DataWarehouse ETL

Este projeto tem como objetivo a criação de um Data Warehouse no PostgreSQL usando Python para extrair dados de arquivos em uma pasta de entrada (`input`) e também coletar dados de um banco de dados MongoDB. O processo de ETL (Extract, Transform, Load) é aplicado para limpar e transformar os dados antes de serem armazenados no PostgreSQL, criando um ambiente centralizado para análise e consulta de dados.

## Pré-Requisitos

- Python 3.9 ou superior
- PostgreSQL
- MongoDB
- Docker
- pymongo 3.12
- sqlalchemy 1.4.26
- pandas 1.3.3

## Criação do Projeto

### Configuração do Ambiente com Docker Compose

Para criar meu ambiente de desenvolvimento, configurei o Docker Compose para incluir o MongoDB, o PostgreSQL e o Python. O Docker Compose facilita a definição e execução de vários containers como serviços interconectados. Abaixo está o arquivo ``docker-compose.yml`` configurado para o projeto:

```yml
version: '3'
services:
  postgres:
    image: postgres:latest
    environment:
      POSTGRES_DB: mydb
      POSTGRES_USER: fzin
      POSTGRES_PASSWORD: puquinha
    ports:
      - "5432:5432"
    networks:
      - etl-network

  mongo:
    image: mongo:latest
    ports:
      - "27017:27017"
    networks:
      - etl-network

  python-app:
    build:
      context: .
      dockerfile: Dockerfile
    volumes:
      - ./input:/app/input
    depends_on:
      - postgres
      - mongo
    networks:
      - etl-network

networks:
  etl-network:
```
Neste arquivo, defini três serviços: PostgreSQL, MongoDB e Python, todos conectados à mesma rede chamada ``etl-network``, permitindo que eles se comuniquem entre si.

### Configuração das Conexões com Bancos de Dados

No meu arquivo ``etl.py``, importei as bibliotecas necessárias para estabelecer conexões com o MongoDB e o PostgreSQL. Configurei essas conexões com base nas informações do meu ambiente, incluindo URLs, nomes de banco de dados e credenciais.

```python
import pymongo
from sqlalchemy import create_engine

# Configuração da conexão com o PostgreSQL
URL_POST = 'postgresql://fzin:puquinha@postgres:5432/mydb'
engine = create_engine(URL_POST)

# Configuração da conexão com o MongoDB
URL_MONGO = "mongodb://root:puquinha1@mongodb"
mongo_client = pymongo.MongoClient(URL_MONGO)
mongo_db = mongo_client['ecommerce']
```
Certifique-se de adaptar essas configurações de acordo com as credenciais e informações específicas do seu ambiente.

### Processo de ETL (Extract, Transform, Load)

O processo de ETL é implementado no arquivo `etl.py`. Extraí dados de arquivos CSV em uma pasta de entrada (./input) e do MongoDB. Em seguida, apliquei transformações necessárias aos dados, como seleção de colunas e mesclagem de dataframes, e carreguei esses dados em tabelas fato e dimensão no PostgreSQL.

Aqui está um exemplo simplificado de como extraí e carreguei dados do MongoDB e de arquivos CSV:

```python
# Extração de dados do MongoDB
df_mongo = mongo_db['order_reviews']
# ...

# Leitura de arquivos CSV
customers = pd.read_csv('input/customers.csv')
order_items = pd.read_csv('input/order_items.csv')
# ...

# Transformações e mesclagem de dataframes
# ...

# Carregamento de dados nas tabelas fato e dimensão no PostgreSQL
tabela_fatos.to_sql('fato_tabela_etl', engine, if_exists='replace', index=False)
customers.to_sql('dim_customers', engine, if_exists='replace', index=False)
order_items.to_sql('dim_order_items', engine, if_exists='replace', index=False)
# ...
```
No meu processo de ETL real, apliquei transformações específicas necessárias para preparar os dados antes de inseri-los nas tabelas do PostgreSQL. Essas transformações incluíram limpeza de dados, formatação, agregações e outras manipulações.

Esta seção descreve como criei meu projeto, desde a configuração do ambiente com Docker Compose até a implementação do processo de ETL para consolidar os dados no PostgreSQL. Certifiquei-me de documentar quaisquer outras etapas ou detalhes específicos do meu projeto, conforme necessário.


## Autor

[Fabrício Ramos](https://github.com/fzin1)


## Notas de Versão

Versão 1.0.0 (20/09/2023): Criação do projeto





