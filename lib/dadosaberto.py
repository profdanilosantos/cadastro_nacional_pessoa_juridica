dict_file_header = {
    'CNAECSV': "cnae",
    'EMPRECSV': "empresa",
    'ESTABELE': "estabelecimento",
    'MOTICSV': "motivo_situacao_cadastral",
    'MUNICCSV': "municipio",
    'NATJUCSV': "natureza_juridica",
    'PAISCSV': "pais",
    'QUALSCSV': "qualificacao_socio",
    'SOCIOCSV': "socio",
    'SIMPLES': "simples",
}


# Dados para o cabeçalho e as linhas
cabecalho_csv = {
    'cnae' : ['codigo', 'descricao'],
    'pais' : ['codigo','descricao'],
    'municipio' : ['codigo','descricao'],
    'natureza_juridica' :['codigo','descricao'],
    'qualificacao_socio' :['codigo','descricao'],
    'motivo_situacao_cadastral' :['codigo','descricao'],
    'empresa' : ['cnpj_basico', 'razao_social', 'natureza_juridica', 'qualificacao_responsavel', 'capital_social', 'porte_empresa', 'ente_federativo'],
    'estabelecimento' : ['cnpj_basico', 'cnpj_ordem','cnpj_dv','identificador','nome_fantasia','situacacao_cadastral','data_situacao_cadastral',
                          'codigo_motivo_situacao_cadastral','cidade_exterior','codigo_pais','data_inicio_atividade','cnae_principal','cnae_secundario',
                          'tipo_logradouro','logradouro','numero','complemento','bairro','cep','uf','municipio',
                          'ddd1','telefone1','ddd2','telefone2','ddd3','telefone3','email','situacao_especial','data_situacao_especial'],
    'simples' : ['cnpj_basico','opcao_simples','data_opcao_simples','data_exclusao_simples','opcao_mei','data_opcao_mei','data_exclusao_mei'],
    'socio' : ['cnpj_basico','identificador','nome','cpfcnpj','codigo_qualificacao_socio','data_entrada_sociedade','codigo_pais','representante_cpf',
                'representante_nome','representante_codigo_qualificacao','codigo_faixa_etaria'],
}

file_csv = {
    'cnae' : 'CNAECSV',
    'pais' : 'PAISCSV',
    'municipio' : 'MUNICCSV',
    'natureza_juridica' : 'NATJUCSV',
    'qualificacao_socio' : 'QUALSCSV',
    'motivo_situacao_cadastral' : 'MOTICSV',
    'empresa' : 'EMPRECSV',
    'estabelecimento' : 'ESTABELE',
    'simples' : 'SIMPLES.CSV',
    'socio' : 'SOCIOCSV',
}

parquet_01 = {
    'cnae',
    'pais',
    'municipio',
    'natureza_juridica',
    'qualificacao_socio',
    'motivo_situacao_cadastral'
}


parquet_02 = {
    'empresa',
    'estabelecimento',
    'simples',
    'socio',
}


# Cnaes.zip
# Empresas0.zip
# Estabelecimentos0.zip
# Motivos.zip
# Municipios.zip
# Naturezas.zip
# Paises.zip
# Qualificacoes.zip
# Simples.zip
# Socios0.zip


porte_empresa = {
    '00' : 'NÃO INFORMADO',
    '01' : 'MICRO EMPRESA',
    '03' : 'EMPRESA DE PEQUENO PORTE',
    '05' : 'DEMAIS',
}

identificador = {
    1:'MATRIZ',
    2:'FILIAL'
}

situacao_cadastral = {
    '01':'NULA',
    '2':'ATIVA',
    '3':'SUSPENSA',
    '4':'INAPTA',
    '08':'BAIXADA'
}

opcao_simples = {
    'S': 'SIM',
    'N': 'NÃO',
    '' : 'OUTROS'
}

opcao_mei = {
    'S': 'SIM',
    'N': 'NÃO',
    '' : 'OUTROS'
}

identificador_socio = {
    1 :'PESSOA JURÍDICA',
    2 :'PESSOA FÍSICA',
    3 :'ESTRANGEIRO'
}

faixa_etaria = {
    '1':'para os intervalos entre 0 a 12 anos',
    '2':'para os intervalos entre 13 a 20 anos',
    '3':'para os intervalos entre 21 a 30 anos',
    '4':'para os intervalos entre 31 a 40 anos',
    '5':'para os intervalos entre 41 a 50 anos',
    '6':'para os intervalos entre 51 a 60 anos',
    '7':'para os intervalos entre 61 a 70 anos',
    '8':'para os intervalos entre 71 a 80 anos',
    '9':'para maiores de 80 anos',
    '0':'para não se aplica'
}
