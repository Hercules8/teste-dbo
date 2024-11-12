import mysql.connector
import requests
import time

# Dados de conexão MySQL (ajuste conforme necessário para a hospedagem)
try:
    conexao = mysql.connector.connect(
        host='localhost',
        user='dantorto_console',
        password='Danilo2024',
        database='dantorto_ceps'
    )
    print("Conexão com o banco de dados estabelecida com sucesso.")
except mysql.connector.Error as err:
    print(f"Erro ao conectar ao banco de dados: {err}")
    exit()

cursor = conexao.cursor()

# Seleciona as linhas que precisam de consulta, agora em ordem decrescente
cursor.execute("SELECT id, cep FROM cep_unificado WHERE coordenadas IS NULL AND DDD IS NULL ORDER BY id DESC")
linhas = cursor.fetchall()

if not linhas:
    print("Não há linhas para processar.")
    exit()

# Contador para exibir progresso
linha_atual = 1
total_linhas = len(linhas)

for linha in linhas:
    id, cep = linha
    print(f"Consultando linha {linha_atual} de {total_linhas}...")

    url = f"https://cep.awesomeapi.com.br/json/{cep}"
    try:
        resposta = requests.get(url)
        resposta.raise_for_status()  # Verifica se houve erro na requisição
        dados = resposta.json()

        # Verifica se a resposta contém as informações desejadas
        if 'lat' in dados and 'lng' in dados and 'ddd' in dados:
            coordenadas = f"POINT({dados['lng']} {dados['lat']})"
            ddd = int(dados['ddd'])

            # Atualiza a linha na tabela MySQL
            cursor.execute(
                "UPDATE cep_unificado SET coordenadas = ST_GeomFromText(%s), DDD = %s WHERE id = %s",
                (coordenadas, ddd, id)
            )
            conexao.commit()
            print(f"Linha {linha_atual} atualizada com sucesso.")
        else:
            print(f"Dados insuficientes para a linha {linha_atual}, nenhum dado gravado.")

    except requests.exceptions.RequestException as e:
        print(f"Erro ao consultar o CEP {cep} na linha {linha_atual}: {e}")
        time.sleep(10)  # Pausa de 10 segundos em caso de erro

    except mysql.connector.Error as err:
        print(f"Erro ao executar consulta SQL para a linha {linha_atual}: {err}")
        conexao.rollback()
        time.sleep(10)

    linha_atual += 1

print("Consulta finalizada. Todas as linhas processadas.")
cursor.close()
conexao.close()
