import pymongo
import pandas as pd
import psycopg2
from sqlalchemy import create_engine

# Estabeleça a conexão com o MongoDB
client = pymongo.MongoClient("mongodb://root:puquinha1@mongodb")  # Substitua pela URL do seu MongoDB
db = client["ecommerce"]  # Substitua pelo nome do seu banco de dados
collection = db["order_reviews"]  # Substitua pelo nome da sua coleção
data = list(collection.find())  # Recupere todos os documentos da coleção
df = pd.DataFrame(data)  # Crie um DataFrame do Pandas com os dados

# Conecte-se ao PostgreSQL
def conecta_db_post(): # cria uma função para conectar ao postgre
    try: 
        con = psycopg2.connect(
          'postgresql://fzin:puquinha@postgres-etl:5432/mydb'
        )
        print("Conexão bem-sucedida")
        return con
    except Exception as e:
        print(f"Erro na conexão: {str(e)}")
        return None

con = conecta_db_post() 

# Até aqui ta funcionando 

if con:
    try:
        # Crie um mecanismo SQLAlchemy para usar o Pandas dentro do postgre
        engine = create_engine('postgresql://fzin:puquinha@postgres-etl:5432/mydb')
        
        # Substitua 'sua_tabela' pelo nome da tabela PostgreSQL em que deseja inserir os dados
        table_name = 'mydb'
        
        # Use o método to_sql do Pandas para inserir os dados no PostgreSQL
        df.to_sql(table_name, engine, if_exists='replace', index=False)
        print(f'Dados inseridos em {table_name}')
    except Exception as e:
        print(f"Erro ao inserir dados no PostgreSQL: {str(e)}")
    finally:
        con.close()
# Neste código, estamos usando o SQLAlchemy para criar um mecanismo de banco de dados e, em seguida, usando o método to_sql do Pandas para inserir o DataFrame no PostgreSQL. Certifique-se de substituir 'sua_tabela' pelo nome da tabela PostgreSQL em que você deseja inserir os dados. Além disso, ajuste as informações de conexão com o PostgreSQL conforme necessário. Certifique-se de que o SQLAlchemy esteja instalado em seu ambiente (você pode instalá-lo com pip install sqlalchemy).





