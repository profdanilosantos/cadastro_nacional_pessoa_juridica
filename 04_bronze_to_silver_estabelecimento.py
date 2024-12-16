import os
import lib.dadosaberto as da
import lib.functions as f
import config.config as cfg
from datetime import datetime

import duckdb


db_file = cfg.db_file


# Leitura do arquivo CSV separado por ;
conn = duckdb.connect(db_file)

query = f"SELECT uf FROM './data/diversos/estados.csv' "
result = conn.execute(query).fetchall()
for dados in result:
	estado = dados[0]

	query = f'DROP TABLE IF EXISTS silver_estabelecimento_{estado}'
	print(query)
	conn.execute(query)

	query = f'''CREATE TABLE silver_estabelecimento_{estado}
	AS
	SELECT row_number() OVER() AS id,a.* FROM (
		SELECT * EXCLUDE (cidade_exterior,codigo_pais) FROM bronze_estabelecimento_0 WHERE uf = '{estado}'
		UNION
		SELECT * EXCLUDE (cidade_exterior,codigo_pais) FROM bronze_estabelecimento_1 WHERE uf = '{estado}'
		UNION
		SELECT * EXCLUDE (cidade_exterior,codigo_pais) FROM bronze_estabelecimento_2 WHERE uf = '{estado}'
		UNION
		SELECT * EXCLUDE (cidade_exterior,codigo_pais) FROM bronze_estabelecimento_3 WHERE uf = '{estado}'
		UNION
		SELECT * EXCLUDE (cidade_exterior,codigo_pais) FROM bronze_estabelecimento_4 WHERE uf = '{estado}'
		UNION
		SELECT * EXCLUDE (cidade_exterior,codigo_pais) FROM bronze_estabelecimento_5 WHERE uf = '{estado}'
		UNION
		SELECT * EXCLUDE (cidade_exterior,codigo_pais) FROM bronze_estabelecimento_6 WHERE uf = '{estado}'
		UNION
		SELECT * EXCLUDE (cidade_exterior,codigo_pais) FROM bronze_estabelecimento_7 WHERE uf = '{estado}'
		UNION
		SELECT * EXCLUDE (cidade_exterior,codigo_pais) FROM bronze_estabelecimento_8 WHERE uf = '{estado}'
		UNION
		SELECT * EXCLUDE (cidade_exterior,codigo_pais) FROM bronze_estabelecimento_9 WHERE uf = '{estado}'
	) a
	'''
	print(query)
	conn.execute(query)

conn.close()