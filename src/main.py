# Importando os códigos que contem a lógica do projeto
# Esse import, serve para receber o arquivo de log que está como
# txt e converter ele para json com o padrão de um arquivo json
from src.utils import handle_json

# Import database
from src.db import database

# Import do arquivo que extrai os dados do arquivo json
from src.ETL import extract

# Import do arquivo que faz as devidas transformações nos dados
from src.ETL import transform

# Import do arquivo que faz a carga para o banco de dados
from src.ETL import load

# Import do arquivo que gera o report .csv final
from src.reports import reports

# O arquivo original que será convertido
text_file = '../data/logs.txt'
# O arquivo final, convertido para json
json_file = '../data/logs.json'

# Chamar a classe e chamando os metodos para ler o arquivo original e escrever ele para o arquivo json
HJ = handle_json.Handle_json(text_file, json_file)
HJ.read_file()
HJ.write_file()

# Chamar a classe e o metodo do arquivo que extrai os dados do arquivo json
EX = extract.Extract(json_file)
EX.create_dataframe()

# Chamar a classe e o metodo do arquivo que faz as  transformações dos dados
TS = transform.Transform(EX.DF)
TS.transform_data()

# iniciar conexão com o banco de dados e criar as tabelas se não existir
db = database.Database()
db.create_tables()

# Fazer a carga dos dados para o banco de dados
# Ao executar esse arquivo, ele demora um pouco devido ao modo do pandas fazer o insert e devido ao chunksize baixo para não sobrecarregar o meu computador, você pode testar colocando um número maior
LD = load.Load(db.engine, db.metadata, TS.request_DF, TS.routes_DF, TS.services_DF, TS.log_DF)
LD.inserting_into_tables()

# Gerar os reports finais
RP = reports.Reports(db.engine, '../output/')
RP.generate_reports()
