import pymongo
import pandas as pd
import psycopg2

# Estabeleça a conexão com o MongoDB
client = pymongo.MongoClient("mongodb://root:puquinha1@mongodb")  # Substitua pela URL do seu MongoDB
db = client["ecommerce"]  # Substitua pelo nome do seu banco de dados
collection = db["order_reviews"]  # Substitua pelo nome da sua coleção

# Recupere os dados da coleção do MongoDB e carregue-os em um DataFrame do Pandas
data = list(collection.find())  # Recupere todos os documentos da coleção
df = pd.DataFrame(data)  # Crie um DataFrame do Pandas com os dados

# Agora você pode trabalhar com os dados do MongoDB usando o DataFrame df
# Até aqui ta funcionando 

# Conecte-se ao PostgreSQL
def conecta_db():
    try:
        con = psycopg2.connect(
            host='localhost',
            database='mydb',
            user='root',  # Usuário PostgreSQL definido no seu docker-compose.yml
            password='puquinha'  # Senha PostgreSQL definida no seu docker-compose.yml
        )
        print("Conexão bem-sucedida")
        return con
    except Exception as e:
        print(f"Erro na conexão: {str(e)}")
        return None

con = conecta_db()



