# Script que faz o processo de carga e analytics dos dados

# Importações

import pandas as pd
import csv
from cassandra.cluster import Cluster


# Lembrando que esse arquivo não deve ser executado diretamente

if __name__ == '__main__':
    print('Não execute esse arquivo diretamente! Execute o arquivo etl_app.py')


# Arquivo de dados que será carregado no cluster

file_name = 'resultado/dataset_completo.csv'

# Criando o cluster Cassandra

def conecta_cluster():

    # Cria a conexão com o cluster

    cluster = Cluster(['localhost'])

    # Conectando no cluster

    session = cluster.connect()

    # Criando a Keyspace

    session.execute("CREATE KEYSPACE IF NOT EXISTS projeto1 WITH REPLICATION = "
                    "{'class': 'SimpleStrategy', 'replication_factor' : 1}")
    

    # Definindo o tipo da keyspace (uma espécie de commit)

    session.set_keyspace('projeto1')

    # Executando os métodos analytics

    pipeline_analytics_1(session)
    pipeline_analytics_2(session)
    pipeline_analytics_3(session)

    # Fazendo o delete das tabelas após o analytics

    deleta_tabelas(session)

    # Encerrando a sessão e o cluster
    session.shutdown()
    cluster.shutdown()


    print('\nPipeline concluído com sucesso!\n')


#1ª analytics

# Pipeline de Analytics 1 - Busca o artista e o comprimento (tempo) da música do sessionId = 436 e itemInSession = 12

def pipeline_analytics_1(session):

    print('\nInicializando o pipeline de analytics 1...')

    # Criando a tabela

    query = "CREATE TABLE IF NOT EXISTS tb_session_itemSession"

    # Criando as colunas

    query = query + "(sessionId text, itemInSession text, song text, artist text, length text, " \
                    "PRIMARY KEY (sessionId, itemInsession))"
    
    # Fazendo a execução da query

    session.execute(query)


    # Abrindo o arquivo de entrada para a carga de dados

    with open(file_name, 'r', encoding= 'utf8') as fh:
        # Lendo o arquivo
        reader = csv.reader(fh)
        next(reader)

        # Loop para fazer a carga de dados

        for line in reader:
            query = "INSERT INTO tb_session_itemSession (sessionId, itemInSession, song, artist, length )"
            query = query + "VALUES (%s, %s, %s, %s, %s)"
            session.execute(query, (line[8], line[3], line[9], line[0], line[5]))


    # Tendo executado a carga dos dados, fazemos agora o analytics

    query = "SELECT artist, length FROM tb_session_itemSession WHERE sessionId = '436' AND itemInSession = '12'"

    # Convertendo o resultado em um dataframe e gravando em disco

    df = pd.DataFrame(list(session.execute(query))) 
    df.to_csv('resultado/pipeline_1.csv', sep = ',', encoding= 'utf8')

    print('\nResultado no analytics 1:\n')
    print(df)



# Pipeline de Analytics 2 - Busca o artista, o nome da música e o usuário do userid = 54 e sessionid = 616

def pipeline_analytics_2(session):

    print('\nIniciando o pipeline analytics 2...')

    query = "CREATE TABLE IF NOT EXISTS tb_user_session"
    query = query + "(userId text,  sessionId text, itemInSession text, artist text, song text, firstName text," \
        "lastName text, PRIMARY KEY ((userId, sessionId), itemInSession))"

    session.execute(query)


    with open(file_name, 'r', encoding= 'utf8') as fh:
        reader = csv.reader(fh)
        next(reader)

        for line in reader:
            query = "INSERT INTO tb_user_session(userId, sessionId, itemInSession, artist, song, firstName, lastName)"
            query = query +"VALUES(%s, %s, %s, %s, %s, %s, %s)"
            session.execute(query, (line[10], line[8], line[3], line[0], line[9], line[1], line[4]))

    
    query = "SELECT artist, song, firstName, lastName FROM tb_user_session WHERE userId = '54' AND sessionId = '616'"

    df = pd.DataFrame(list(session.execute(query)))
    df.to_csv('resultado/pipeline_2.csv', sep = ',', encoding= 'utf8')

    print('\nResultado no analytics 2:\n')
    print(df)


# Pipeline de Analytics 3 - Busca cada usuário que ouviu a música 'The Rhythm Of The Night'

def pipeline_analytics_3(session):

    print('\nIniciando o pipeline analytics 3...')

    query = "CREATE TABLE IF NOT EXISTS tb_user_song"
    query = query + "(song text, userId text, firstName text, lastName text,"\
                    "PRIMARY KEY (song, userId))"
    session.execute(query)

    with open(file_name, 'r', encoding= 'utf8') as fh:
        reader = csv.reader(fh)
        next(reader)

        for line in reader:
            query = "INSERT INTO tb_user_song (song, userId, firstName, lastName)"
            query = query + "VALUES (%s, %s, %s, %s)"
            session.execute(query, (line[9], line[10], line[1], line[4]))
    
    query = "SELECT firstName, lastName FROM tb_user_song WHERE song = 'The Rhythm Of The Night'"

    df = pd.DataFrame(list(session.execute(query)))
    df.to_csv('resultado/pipeline_3.csv', sep = ',', encoding= 'utf8')

    print('\nResultado no analytics 3:\n')
    print(df)


# Função que deleta as tabelas ao final do pipeline

def deleta_tabelas(session):

    query = "DROP TABLE tb_session_itemSession"
    session.execute(query)

    query = "DROP TABLE tb_user_session"
    session.execute(query)

    query = "DROP TABLE tb_user_song"
    session.execute(query)













    



