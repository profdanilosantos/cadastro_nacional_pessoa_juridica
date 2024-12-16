import duckdb
import os
import pandas as pd
import chardet
import csv

import lib.dadosaberto as da

def drop_table(db_file, table_name):
    # Conecta ao banco de dados DuckDB (cria se não existir)
    con = duckdb.connect(db_file)

    try:
        # Cria a tabela e importa os dados do CSV
        query = f"""
        DROP TABLE IF EXISTS {table_name}
        """
        con.execute(query)

    except Exception as e:
        print(f"Erro ao importar dados: {e}")

    finally:
        # Fecha a conexão
        con.close()


def drop_all_table(db_file,folder_csv,dict_csv,medalhao=''):
    for filename in os.listdir(folder_csv):
        for file in dict_csv:
            if dict_csv[file] in filename:
                drop_table(db_file, medalhao+file)



def adicionar_cabecalho_csv_pd(file_original,file_novo,cabecalhos):
    # Leitura do arquivo CSV separado por ;
    df = pd.read_csv(file_original, sep=';', header=None)

    # Aplicação dos cabeçalhos
    df.columns = cabecalhos

    # Salvar o DataFrame com o novo cabeçalho
    df.to_csv(file_novo, index=False, sep=';')

    print("Arquivo CSV atualizado com os novos cabeçalhos!")


def adicionar_cabecalho_csv(file_original,file_novo,cabecalhos):

    # Leitura do arquivo CSV separado por ;
    conn = duckdb.connect()
    df = conn.execute(f"SELECT * FROM read_csv_auto('{file_original}', delim=';', header=False)").fetchdf()

    # Aplicação dos cabeçalhos
    df.columns = cabecalhos

    # Salvar o DataFrame com o novo cabeçalho
    df.to_csv(file_novo, index=False, sep=';')

    conn.close()


def gerar_cabecalho_csv(nome_arquivo,cabecalhos):

    # Escrever dados no arquivo CSV
    with open(nome_arquivo, mode='w', newline='') as arquivo_csv:
        writer = csv.writer(arquivo_csv, delimiter=';')
        writer.writerow(cabecalhos)

def gerar_cabecalho_csv_all(folder_csv,folder_csvheader,cabecalho_csv,file_csv):
    # Iterar sobre todos os arquivos na pasta
    for filename in os.listdir(folder_csv):
        for file in file_csv:
            if file_csv[file] in filename:
                file_header = os.path.join(folder_csvheader, file)+'.csv'
                gerar_cabecalho_csv(file_header,cabecalho_csv[file])

def gerar_parquet(csv_file,parquet_file,colunas):
    # Conectando ao DuckDB
    conn = duckdb.connect()

    # Leitura do arquivo CSV
    conn.execute(f"DROP TABLE IF EXISTS dados")


    # Leitura do arquivo CSV com os cabeçalhos reais
    conn.execute(f"CREATE TABLE dados AS SELECT {colunas} FROM read_csv_auto('{csv_file}',ignore_errors=true)")

    # Gravação no formato Parquet
    conn.execute(f"COPY dados TO '{parquet_file}' (FORMAT PARQUET)")
    conn.close()


def gerar_parquetdf(csv_file,parquet_file,cabecalhos):
    # Conectando ao DuckDB
    conn = duckdb.connect()

    # Leitura do arquivo CSV sem cabeçalhos
    query = f"SELECT * FROM read_csv_auto('{csv_file}', header=False, ignore_errors=true)"
    print(query)
    df = conn.execute(query).fetchdf()

    # Aplicação dos cabeçalhos
    df.columns = cabecalhos

    # Leitura do arquivo CSV com os cabeçalhos reais
    query = "CREATE TABLE dados AS SELECT * FROM df"
    print(query)
    conn.execute(query)

    # Gravação no formato Parquet
    query = f"COPY dados TO '{parquet_file}' (FORMAT PARQUET)"
    print(query)
    conn.execute(query)

    print(f"Arquivo Parquet {parquet_file} criado com sucesso!")
    conn.close()



def csv_to_table(db_file,csv_file,tabela,create=''):
    # Conectando ao DuckDB
    conn = duckdb.connect(db_file)


    # Leitura do arquivo CSV sem cabeçalhos
    query_csv = f'''
            SELECT * FROM read_csv_auto('{csv_file}', header=False, ignore_errors=true)
        '''

    if create == 'createselect':
        query = f"CREATE TABLE {tabela} AS {query_csv}"
        print(query)
        conn.execute(query)
    elif create == 'createinsert':
        query = f'''CREATE TABLE IF NOT EXISTS {tabela}
                    (codigo VARCHAR,
                    descricao VARCHAR);
                '''
        conn.execute(query)
        query = f"INSERT INTO {tabela}  {query_csv}"
        conn.execute(query)
    conn.close()


def parquet_to_table(db_file,parquet_file,tabela):
    # Conectando ao DuckDB
    print(db_file)
    conn = duckdb.connect(db_file)

    # Leitura do arquivo CSV sem cabeçalhos
    query = f"DROP TABLE IF EXISTS {tabela} "
    conn.execute(query)


    # Leitura do arquivo CSV sem cabeçalhos
    query = f"CREATE TABLE {tabela} AS SELECT * FROM '{parquet_file}' "
    print(query)

    conn.execute(query)
    conn.close()



def copy_csv_to_parquet(csv_file,parquet_file):
    # Conectando ao DuckDB
    conn = duckdb.connect()

    # Gravação no formato Parquet
    query = f"COPY '{csv_file}' TO '{parquet_file}' (FORMAT PARQUET)"
    print(query)
    conn.execute(query)

    conn.close()



def copy_csv_to_parquet2(csv_file,parquet_file):
    # Conectando ao DuckDB
    conn = duckdb.connect()


    # Leitura do arquivo CSV sem cabeçalhos
    query = f"""
        CREATE TABLE dados AS
        SELECT * FROM read_csv_auto(
            '{csv_file}',
            header=True,
            delim=';',
            ignore_errors=True,
            types={{'ddd2': 'VARCHAR'}}
        )
    """
    conn.execute(query)
    # Gravação no formato Parquet
    query = f"COPY 'dados' TO '{parquet_file}' (FORMAT PARQUET)"
    print(query)
    conn.execute(query)

    conn.close()



def copy_csv_to_parquet3(csv_file,parquet_file):
    # Conectando ao DuckDB
    conn = duckdb.connect()


    # Leitura do arquivo CSV sem cabeçalhos
    query = f"""
        CREATE TABLE dados AS
        SELECT * FROM read_csv_auto(
            '{csv_file}',
            header=True,
            delim=';',
            ignore_errors=True,
            types={{'telefone2': 'VARCHAR'}}
        )
    """
    conn.execute(query)
    # Gravação no formato Parquet
    query = f"COPY 'dados' TO '{parquet_file}' (FORMAT PARQUET)"
    print(query)
    conn.execute(query)

    conn.close()


def detectar_codificacao(arquivo):
    with open(arquivo, 'rb') as f:
        resultado = chardet.detect(f.read())
    return resultado['encoding']


def formatar_em_milhoes(numero):
    print(type(numero))
    return f'{numero / 1_000_000:.2f}M'



def filetxt_to_list(filetxt):
    # Inicializar uma lista para armazenar os dados
    dados_lista = []
    # Ler o arquivo de texto
    with open(filetxt, 'r', encoding='utf-8') as file:
        for linha in file:
            # Remover espaços em branco e adicionar a linha à lista
            dados_lista.append(linha.strip())
            # Exibir os dados lidos print
    return dados_lista


def csv_to_header(csv_file_path):
    # Ler a primeira linha do arquivo CSV para obter o cabeçalho
    with open(csv_file_path, 'r', encoding='utf-8') as file:
        header_line = file.readline().strip()
    # Separar o cabeçalho usando o delimitador ;
    columns = header_line.split(';')
    return columns