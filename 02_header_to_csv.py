import os
import lib.dadosaberto as da
import lib.functions as f
import config.config as cfg
from datetime import datetime
import duckdb
import pandas as pd


data_atual = datetime.now()
print(f'Inicio: {data_atual}')

folder_csv = cfg.folder_csv
folder_csvheader = cfg.folder_csvheader
folder_parquet = cfg.folder_parquet




# Iterar sobre todos os arquivos na pasta
for filename in os.listdir(folder_csv):
    part_name = filename.split('.')
    if part_name[2] == 'SIMPLES':
        part_name[3] = 'SIMPLES'


    if '$' in part_name[1]:
        order_file = ''
    else:
        order_file = '_'+part_name[1][-1]


    table_name = f'{da.dict_file_header[part_name[3]]}{order_file}'

    # Definir cabeçalhos para o CSV
    file_header = f'{folder_csvheader}{da.dict_file_header[part_name[3]]}.csv'
    columns = f.csv_to_header(file_header)


    # Caminho do arquivo CSV de entrada e Parquet de saída
    csv_file_path = f'{folder_csv}{filename}'
    print('-'*50)
    print(csv_file_path)
    parquet_file_path = f'{folder_parquet}{da.dict_file_header[part_name[3]]}{order_file}.parquet'


    # Ler o CSV sem cabeçalho e com codificação específica
    try:
        print('Start DF')
        df = pd.read_csv(csv_file_path, header=None, names=columns, encoding='ISO-8859-1', sep=';', on_bad_lines='skip', low_memory=False)
        print('Stop DF')

        # Ler o arquivo CSV diretamente em um DataFrame pandas usando DuckDB
        #df = duckdb.query(f"SELECT * FROM read_csv_auto('{csv_file_path}', delim=';', header=True, encoding='UTF-8')").to_df()
        # Exibir as primeiras linhas do DataFrame print


    except pd.errors.ParserError as e:
        print(f"Erro ao analisar o arquivo CSV: {e}")

    # Conectar ao DuckDB e criar uma tabela a partir do DataFrame
    conn = duckdb.connect(database=':memory:')
    conn.register('df', df)

    # Converter a tabela para Parquet
    conn.execute(f"COPY df TO '{parquet_file_path}' (FORMAT PARQUET)")
    print(f"Arquivo Parquet gerado: {parquet_file_path}")

    df_parquet = conn.execute(f"SELECT * FROM '{parquet_file_path}'").fetchdf()

    print(df_parquet)
    # Verificar se o arquivo parquet existe
    if os.path.exists(parquet_file_path):
        print(f"O arquivo {parquet_file_path} existe.")
        # Deletar o arquivo CSV
        os.remove(csv_file_path)
        print(f"O arquivo {csv_file_path} foi deletado com sucesso.")

    # Fechar a conexão
    conn.close()
    print('-'*60)
