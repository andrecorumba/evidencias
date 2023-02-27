import sqlite3
import pandas as pd
import os

def copiar_tabela_sqlite(origem, destino, tabela):
    # Conectar ao banco de dados de origem
    conn_origem = sqlite3.connect(origem)

    # Ler a tabela de origem com pandas
    df = pd.read_sql_query(f"SELECT * FROM {tabela}", conn_origem)

    # Conectar ao banco de dados de destino
    conn_destino = sqlite3.connect(destino)

    # Salvar o DataFrame como uma tabela no banco de dados de destino
    df.to_sql(tabela, conn_destino, if_exists="replace", index=False)

    # Fechar as conexões com os bancos de dados
    conn_origem.close()
    conn_destino.close()


def corga_tabela_csv(arquivo_csv, destino, tabela):
    # Conectar ao banco de dados de destino
    conn_destino = sqlite3.connect(destino)
    cursor_destino = conn_destino.cursor()

    # Ler o arquivo CSV
    df = pd.read_csv(arquivo_csv)

    # Salvar o DataFrame como uma tabela no banco de dados de destino
    df.to_sql(tabela, conn_destino, if_exists="replace", index=False)

    # Fechar a conexão com os bancos de dados
    conn_destino.close()

def carga_tabela_arquivos_binarios(caminho_pasta, destino, tabela):
    # Criar um DataFrame vazio para armazenar as informações dos arquivos
    df = pd.DataFrame(columns=["nome", "conteudo"])
    
    # Percorrer a lista de arquivos na pasta
    for arquivo in os.listdir(caminho_pasta):
        # Ignorar pastas e arquivos ocultos
        if os.path.isfile(os.path.join(caminho_pasta, arquivo)) and not arquivo.startswith("."):
            # Ler o conteúdo do arquivo em binário
            with open(os.path.join(caminho_pasta, arquivo), "rb") as f:
                conteudo = f.read()

            # Adicionar o nome e o conteúdo do arquivo ao DataFrame
            df = pd.concat([df, pd.DataFrame({"nome": [arquivo], "conteudo": [conteudo]})], ignore_index=True)
    
    # Conectar ao banco de dados de destino
    conn = sqlite3.connect(destino)
    
    # Gravar o DataFrame na tabela tb_arquivos do banco de dados de destino
    df.to_sql(tabela, conn, if_exists="replace", index=False)
    
    # Fechar a conexão com o banco de dados
    conn.close()

def restaurar_arquivos(bd_origem, tabela_origem, pasta_destino, lista_arquivos):
    conn = sqlite3.connect(bd_origem)
    cursor = conn.cursor()
    
    if lista_arquivos:
        arquivos = ",".join([f"'{arquivo}'" for arquivo in lista_arquivos])
        query = f"SELECT nome, conteudo FROM {tabela_origem} WHERE nome IN ({arquivos})"
    
    else:
        query = f"SELECT nome, conteudo FROM {tabela_origem}"
    
    cursor.execute(query)
    
    for row in cursor.fetchall():
        nome_arquivo = row[0]
        conteudo_arquivo = row[1]

        with open(os.path.join(pasta_destino, nome_arquivo), "wb") as f:
            f.write(conteudo_arquivo)
    
    conn.close()


def main():
    # Parâmetros
    origem = "/Users/andreluiz/Downloads/AA_254-2021-SR-GO ITEM_03/files/Database/msgstore.db"
    bd_operacao = "/Users/andreluiz/Downloads/cgu_201899031_145.db"
    arquivo_csv = "/Volumes/Seagate Expansion Drive/Anexo Laudo 437_2021 Proced_2020.0030152-SR-PF-MS AA_254-2021-SR-PF-GO ITENS_01-02-03-04-08-11/AA_254-2021-SR-GO ITEM_03/IPED/Lista de Arquivos.csv"
    pasta_audio = "/Users/andreluiz/Downloads/AA_254-2021-SR-GO ITEM_03/files/Audio"
    pasta_destino = "/Users/andreluiz/Downloads"
    lista_arquivos = ['708b120da428998701c1f61903afda76.mp3']
    tabela_origem = "tb_audios_arquivos_binarios"

    # Restaurando arquivos
    #restaurar_arquivos(bd_operacao,tabela_origem, pasta_destino, lista_arquivos)

    # Copiar tabela do banco de dados de origem para o de destino
    #print("Iniciando Carga...")
    

    #try:
        #copiar_tabela_sqlite(origem, bd_operacao, "messages")
        # copiar_tabela_sqlite(origem, bd_operacao, "wa")  
        # carga_tabela_csv(arquivo_csv, bd_operacao, "tb_iped_lista_arquivos")
     #   carga_tabela_arquivos_binarios(pasta_audio, bd_operacao, "tb_audios_arquivos_binarios")
     #   print("carga tabela wa completa!")
    #except Exception as e:
    #    print("Não conseguiu fazer a carga da tabela")

if __name__ == "__main__":
    main()
