# Script que realiza a extração e tranformação dos dados

#Importações

import os      # manipula o sistema operacional
import glob    # busca o caminho completo dos arquivos
import csv      
from pipeline import conecta_cluster


# Função que irá realizar a extração dos dados

def etl_processa_arquivo():

    print("\nIniciando a etapa de extracao do ETL...")

    # Pasta onde estão os dados a serem processados

    current_path = "dados"
    print("\nOs arquivo serao processados na pasta " + current_path)

    # Lista com os caminhos de cada arquivo

    lista_caminho_arquivo = []

    # preenchendo a lista

    # O loop irá percorrer toda a hierarquia, o python saberá qual é essa hirarquia através da função walk

    print("\nExtraindo o caminho de cada arquivo...")

    for root, dirs, files in os.walk(current_path):

        lista_caminho_arquivo = glob.glob(os.path.join(root, '*')) 
        # O * indica que o programa pegará todo o caminho até a pasta do arquivo
        # partindo da pasta raiz (root) até onde estão os dados.
    
    # Lista para manipular as linhas dos arquivos
    linhas_dados_all = list()

    # O objetivo aqui é concatenar o conteúdo dos arquivos

    print("\nExtraindo as linhas de cada arquivo e concatenando em um unico arquivo...")

    for file in lista_caminho_arquivo:
        #lendo os arquivos
        with open(file, 'r', encoding= 'utf8', newline= '') as fh: # newline é uma nova linha ao final de cada registro
            reader = csv.reader(fh)
            next(reader)  # pula a primeira linha (header)

            # Fazendo o append de cada linha
            for line in reader:
                linhas_dados_all.append(line) 
                # Ao final da execução essa lista terá todas as linhas de todos os arquivos

    
    print('\nEtapa de extracao dos dados finalizada com sucesso!')

    return linhas_dados_all

# Função que realiza a tranformação dos dados

def etl_processa_dados(records):

    print('\nIniciando o processo de transfomacao do ETL...')

    # Registrando uma estrutura de dados
    # Cria um objeto com todo o conteudo do csv
    # Um espécie de massa de dados de texto (há muito texto nos dados)

    csv.register_dialect('DadosGerais', quoting= csv.QUOTE_ALL, skipinitialspace= True)

    print('\nFiltrando os dados relevantes que serao inseridos no Apache Cassandra.')

    with open('resultado/dataset_completo.csv', 'w', encoding= 'utf8', newline= '') as fh:
        # Criando o objeto:
        writer = csv.writer(fh)

        # Executando
        writer.writerow(['artist', 'firstName', 'itemInSession', 'lastName', 'length', 'level', 'location', 'sessionId', 'song', 'userId'])

        # Percorrendo a lista e gravando no arquivo final

        for row in records:
            # Se não houver artista não gera linha final
            if not row[0]:

                continue
            # Grava linha de dados

            writer.writerow((row[0], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[12], row[13], row[16]))

        print('\nEtapa de transformacao finalizada com sucesso!')

    
# Bloco main

if __name__ == '__main__':

    # Etapa 1 do ETL 
    record_list = etl_processa_arquivo()

    # Se a etapa 1 foi concluída com sucesso segue-se a etapada 2:

    if record_list: # Isto é, se record_list != null

        # Etapa 2 do ETL

        etl_processa_dados(record_list)

        # Conecta no cluster Cassandra para a carga

        conecta_cluster()













