import mysql.connector   # Importa a biblioteca para conexão com o banco de dados MySQL
from pylogix import PLC  # Importa a biblioteca para comunicação com o PLC Logix
import schedule          # Importa a biblioteca para agendamento de tarefas
import time              # Importa a biblioteca de tempo para gerenciamento de tempo
import json              # Importa a biblioteca do JSON para arquivos

def read_and_insert_data():
    # Carregar as informações de acesso do arquivo JSON
    with open((r'Grafana\config.json'), 'r') as f:
        config = json.load(f)

    # Configurar a conexão com o banco de dados MySQL
    db_connection = mysql.connector.connect(
        host=config['mysql']['host'],
        user=config['mysql']['user'],
        password=config['mysql']['password'],
        database=config['mysql']['database']
    )

    # Criar um cursor para executar consultas SQL
    cursor = db_connection.cursor()

    # Configurar a conexão com o PLC
    with PLC() as comm:
        comm.IPAddress = '192.168.15.100'

        # Lista de tags que você deseja ler
        tags_to_read = ['Etanol', 'Oleo', 'Acido_Graxo', 'IOP', 'DDGS', 'Energia']

        for tag_name in tags_to_read:
            try:
                ret = comm.Read(tag_name)

                # Inserir os dados lidos na tabela MySQL
                sql = "INSERT INTO grafana (tag_name, tag_value, tag_status) VALUES (%s, %s, %s)"
                val = (ret.TagName, ret.Value, ret.Status)
                cursor.execute(sql, val)

                # Confirmar a transação
                db_connection.commit()

                print("Dados da tag", tag_name, "inseridos com sucesso.")
            except Exception as e:
                print("Erro ao ler ou inserir os dados da tag", tag_name + ":", str(e))

    # Fechar a conexão com o banco de dados
    cursor.close()
    db_connection.close()

# Agendar a função para ser executada a cada 30 segundos
schedule.every(10).seconds.do(read_and_insert_data)

# Loop para manter o programa em execução
while True:
    schedule.run_pending()
    time.sleep(1)
