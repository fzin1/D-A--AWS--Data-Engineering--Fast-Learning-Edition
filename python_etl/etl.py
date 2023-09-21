import pymongo
import pandas as pd
import re
import glob
import os
from sqlalchemy import create_engine

# definindo algumas constantes
URL_POST = 'postgresql://fzin:puquinha@postgres-etl:5432/mydb'
URL_MONGO = "mongodb://root:puquinha1@mongodb"
NOME_BANCO = "ecommerce"
NOME_COLECAO = "order_reviews"
DIRETORIO_CSV = './input'

# cria um dataframe chamado tabela_fatos
tabela_fatos = pd.DataFrame()

# função para fazer a conexão com o postgre e criar tabela
def criar_tabelas(df,nome):  
    try:
        engine = create_engine(URL_POST)
        df.to_sql(nome, engine, if_exists='replace', index=False)
        print(f'Dados inseridos com sucesso na tabela {nome} do PostgreSQL.')
    except Exception as e:
        print(f'Erro ao inserir dados no PostgreSQL: {str(e)}')
        
# função para fazer a conexão com o mongobd, recuperar os dados da coleção e colocar em um dataframe 
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

# chama a função para conectar ao mongo
df_mongo = conectar_mongo(URL_MONGO, NOME_BANCO, NOME_COLECAO)

# usa a biblioteca glob para criar uma lista com os arquivos que estão no diretorio e tem o final .csv
arquivos_csv = glob.glob(os.path.join(DIRETORIO_CSV, '*.csv')) 
arquivos_csv.sort() # organiza a lista para um padrão

# le a variavel arquivos_csv, seleciona uma posição da lista para colocar nas respectivas variaveis assim transformando elas em varios dataframes
customers = pd.read_csv(arquivos_csv[0])
order_items = pd.read_csv(arquivos_csv[1])
order_payments = pd.read_csv(arquivos_csv[2])
orders = pd.read_csv(arquivos_csv[3])
products = pd.read_csv(arquivos_csv[4])

# especifica as colunas que eu quero e adiciono em dataframes especificos que pretendo jogar na tabela fatos
order_id = orders[['order_id','customer_id']]
payment = order_payments[['payment_value','payment_installments','order_id']]
preco_frete = order_items[['product_id','price','freight_value','order_id']]
df_mongo_pg = df_mongo[['review_score','order_id']]

# dou um insert no df order_id na minha tabela fatos pois é o que quero usar como chave pra organizar os demais
tabela_fatos.insert(0, 'order_id', order_id['order_id'])


# tive a ideia de tirar as colunas que já estão na tabela fatos mas optei por manter pra facilitar analises mais detalhadas

# order_payments_delete = ['payment_value','payment_installments']
# order_items_delete = ['product_id','price','freight_value']

# customers.drop('customer_unique_id',axis=1,inplace=True)
# order_payments.drop(order_payments_delete,axis=1,inplace=True)
# order_items.drop(order_items_delete,axis=1,inplace=True)

# dou um merge nos dfs que queria adicionar usando o order_id como chave e usando left pois quero apenas adicionar novas e mantes as que já estão la
tabela_fatos = tabela_fatos.merge(order_id ,on="order_id",how='left')
tabela_fatos = tabela_fatos.merge(preco_frete ,on="order_id",how='left')
tabela_fatos = tabela_fatos.merge(payment ,on="order_id",how='left')
tabela_fatos = tabela_fatos.merge(df_mongo_pg ,on="order_id",how='left')

# chamo a função de criação de tabelas dando como parametros o df e o nome da tabela que quero criar
criar_tabelas(tabela_fatos,"fato_tabela_etl")
criar_tabelas(customers,"dim_customers")
criar_tabelas(order_items,"dim_order_items")
criar_tabelas(order_payments,"dim_order_payments")
criar_tabelas(orders,"dim_orders")
criar_tabelas(products,"dim_products")
    