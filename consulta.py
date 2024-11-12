import mysql.connector
import requests
import time
import yaml

# Carregar o arquivo de configuração
with open("config.yaml", "r") as config_file:
    config = yaml.safe_load(config_file)

# Configurações do banco de dados e API
db_config = config['database']
api_config = config['api']
script_config = config['script']

# Conectar ao banco de dados
try:
    conexao = mysql.connector.connect(
        host=db_config['host'],
        user=db_config['user'],
        password=db_config['password'],
        database=db_config['database_name']
    )
    print("Conexão com o banco de dados estabelecida com sucesso.")
except mysql.connector.Error as err:
    print(f"Erro ao conectar ao banco de dados: {err}")
    exit()

cursor = conexao.cursor()

# Consultar dados, respeitando a direção definida em config
query = f"SELECT id, cep FROM {db_config['table_name']} WHERE coordenadas IS NULL AND DDD IS NULL ORDER BY id {script_config['query_direction']}"
cursor.execute(query)
linhas = cursor.fetchall()

if not linhas:
    print("Não há linhas para processar.")
    exit()

# Processar as linhas e consultar a API
linha_atual = 1
total_linhas = len(linhas)

for linha in linhas:
    id, cep = linha
    print(f"Consultando linha {linha_atual} de {total_linhas}...")

    url = api_config['url_template'].format(cep=cep)
    for tentativa in range(script_config['max_retries']):
        try:
            resposta = requests.get(url)
            resposta.raise_for_status()
            dados = resposta.json()

            if 'lat' in dados and 'lng' in dados and 'ddd' in dados:
                coordenadas = f"POINT({dados['lng']} {dados['lat']})"
                ddd = int(dados['ddd'])

                cursor.execute(
                    f"UPDATE {db_config['table_name']} SET coordenadas = ST_GeomFromText(%s), DDD = %s WHERE id = %s",
                    (coordenadas, ddd, id)
                )
                conexao.commit()
                print(f"Linha {linha_atual} atualizada com sucesso.")
                break

            else:
                print(f"Dados insuficientes para a linha {linha_atual}, nenhum dado gravado.")
                break

        except requests.exceptions.RequestException as e:
            print(f"Erro ao consultar o CEP {cep} na linha {linha_atual}: {e}")
            if tentativa < script_config['max_retries'] - 1:
                print("Tentando novamente...")
                time.sleep(5)  # Pausa antes de nova tentativa
            else:
                print("Número máximo de tentativas atingido. Prosseguindo para a próxima linha.")
                break

    linha_atual += 1
    time.sleep(script_config['sleep_interval'])

print("Consulta finalizada. Todas as linhas processadas.")
cursor.close()
conexao.close()
