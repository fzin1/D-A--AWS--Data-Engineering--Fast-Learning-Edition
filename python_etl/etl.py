import pymongo
import pandas as pd
import psycopg2
import re
import glob
import os
from sqlalchemy import create_engine

URL_POST = 'postgresql://fzin:puquinha@postgres-etl:5432/mydb'

url_mongo = "mongodb://root:puquinha1@mongodb"
nome_banco = "ecommerce"
nome_colecao = "order_reviews"

diretorio = './input'
# função pra criar tabela
def criar_tabelas(df,nome):  
  
    try:
        engine = create_engine(URL_POST)
        df.to_sql(nome, engine, if_exists='replace', index=False)
        print(f'Dados inseridos com sucesso na tabela {nome} do PostgreSQL.')
    except Exception as e:
        print(f'Erro ao inserir dados no PostgreSQL: {str(e)}')
        
# Função para conectar ao MongoDB e recuperar os dados de uma coleção
def conectar_mongo(url, banco, colecao):
    try:
        client = pymongo.MongoClient(url)
        db = client[banco]
        collection = db[colecao]
        data = list(collection.find())
        df_mongo = pd.DataFrame(data)
        print("Conexão com MongoDB bem-sucedida e DataFrame criado.")
        return df_mongo  
    except Exception as e:
        print(f"Erro na conexão com o MongoDB: {str(e)}")
        return None

# Crie um mecanismo de conexão usando a biblioteca SQLAlchemy


# conexão com o mongo
df_mongo = conectar_mongo(url_mongo, nome_banco, nome_colecao)

# csv

arquivos_csv = glob.glob(os.path.join(diretorio, '*.csv'))
arquivos_csv.sort()

# criando a tabela fatos

tabela_fatos = pd.DataFrame()

# criando df para cada arquivo csv
customers = pd.read_csv(arquivos_csv[0])
order_items = pd.read_csv(arquivos_csv[1])
order_payments = pd.read_csv(arquivos_csv[2])
orders = pd.read_csv(arquivos_csv[3])
products = pd.read_csv(arquivos_csv[4])

# separando tudo que vai adicionar na fatos

order_id = orders[['order_id','customer_id']]
payment = order_payments[['payment_value','payment_installments','order_id']]
preco_frete = order_items[['product_id','price','freight_value','order_id']]
df_mongo_pg = df_mongo[['review_score','order_id']]

tabela_fatos.insert(0, 'order_id', order_id['order_id'])

tabela_fatos = tabela_fatos.merge(order_id ,on="order_id",how='left')
tabela_fatos = tabela_fatos.merge(preco_frete ,on="order_id",how='left')
tabela_fatos = tabela_fatos.merge(payment ,on="order_id",how='left')
tabela_fatos = tabela_fatos.merge(df_mongo_pg ,on="order_id",how='left')


criar_tabelas(tabela_fatos,"fato_tabela_etl")
criar_tabelas(customers,"dim_customers")
criar_tabelas(order_items,"dim_order_items")
criar_tabelas(order_payments,"dim_order_payments")
criar_tabelas(orders,"dim_orders")
criar_tabelas(products,"dim_products")