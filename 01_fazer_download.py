import requests
from requests.exceptions import ConnectionError, Timeout, RequestException
from bs4 import BeautifulSoup
import os
import zipfile
import urllib.parse
from datetime import datetime
from tqdm import tqdm
import config.config as cfg




def download_and_extract(url, download_dir, extract_dir,file_log):
    # Criar diretórios se não existirem
    os.makedirs(download_dir, exist_ok=True)
    os.makedirs(extract_dir, exist_ok=True)

    # Baixar a página
    response = requests.get(url)

    soup = BeautifulSoup(response.content, 'html.parser')

    # Encontrar todos os links para arquivos
    for link in soup.find_all('a'):
        file_url = link.get('href')
        if file_url and file_url.endswith('.zip'):  # Ajuste esta condição conforme necessário
            full_url = urllib.parse.urljoin(url, file_url)
            file_name = os.path.join(download_dir, os.path.basename(file_url))
            print('-'*50)
            print(file_url)

            # Iniciar variável ja_baixado com FALSE
            ja_baixado = False

            # Ler o arquivo de texto
            with open(file_log, 'r', encoding='utf-8') as filelog:
                conteudo_log = filelog.read()

            #Verifica se o arquivo esta no log / Já foi Baixado
            if file_url in conteudo_log:
                ja_baixado = True
            else:
                #Verifica se o arquivo esta na pasta / Já foi Baixado
                if os.path.exists(file_name):
                    ja_baixado = True


            # Se ja_baixado for False, fazer o download do arquivo
            if ja_baixado == False:
                data_hora = datetime.now()
                print('-'*60)
                print(f'Inicio: {data_hora}')

                # Baixar o arquivo
                print(f"Baixando {full_url}...")


                file_response = requests.get(full_url)
                with open(file_name, 'wb') as file:
                    file.write(file_response.content)



                # Faz uma requisição GET para o URL
                file_response = requests.get(full_url, stream=True)

                # Obtém o tamanho total do arquivo
                total_size = int(file_response.headers.get('content-length', 0))

                # Abre o arquivo local para escrita em modo binário
                with open(file_name, 'wb') as file, tqdm(
                    desc=file_name,
                    total=total_size,
                    unit='iB',
                    unit_scale=True,
                    unit_divisor=1024,
                ) as progress_bar:
                    for data in file_response.iter_content(chunk_size=1024):
                        size = file.write(data)
                        progress_bar.update(size)



                # Extrair o arquivo se for um zip
                if file_name.endswith('.zip'):
                    print(f"Extraindo {file_name}...")
                    with zipfile.ZipFile(file_name, 'r') as zip_ref:
                        zip_ref.extractall(extract_dir)
                    with open(file_log, 'a', encoding='utf-8') as filelog:
                        filelog.write(str(file_url)+'\n')
                        # Remover arquivo ZIP
                        os.remove(file_name)





    print("Download e extração concluídos!")


# Obtém a data atual
data_atual = datetime.now()


# Formata a data para obter apenas o ano e o mês
ano_mes = data_atual.strftime("%Y-%m")
ano_mes = '2024-11'

print(f"O ano-mês atual é: {ano_mes}")

download_dir =  f'{cfg.folder_download}{ano_mes}'

file_log = f'./log/{ano_mes}.log'

if not os.path.exists(download_dir):
    os.makedirs(download_dir)
    print(f"A pasta '{download_dir}' foi criada.")
else:
    print(f"A pasta '{download_dir}' já existe.")


url = f"https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/{ano_mes}/"
print(url)

try:
    download_and_extract(url,download_dir,cfg.folder_csv,file_log)
except ConnectionError as e:
    print(f"Erro de Conexão: {e}")
except Timeout as e:
    print(f"Erro de Timeout: {e}")
except RequestException as e:
    print(f"Erro de Requisição: {e}")


data_hora = datetime.now()
print('-'*60)
print(f'Finalizou: {data_hora}')
