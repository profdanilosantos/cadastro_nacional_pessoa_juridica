import os
import lib.dadosaberto as da
import lib.functions as f
import config.config as cfg
from datetime import datetime

import duckdb


db_file = cfg.db_file
# Iterar sobre todos os arquivos na pasta
for filename in os.listdir(cfg.folder_parquet):
    print('-'*60)
    print(filename)
    tabela = 'bronze_'+filename.replace('.parquet','')
    print(tabela)
    parquet_file = os.path.join(cfg.folder_parquet, filename)
    f.parquet_to_table(db_file,parquet_file,tabela)
