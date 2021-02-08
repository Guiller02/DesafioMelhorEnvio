DesafioMelhorEnvio
Table of contents
Informações gerais
Arquitetura do projeto
Tecnologias
Como utilizar o projeto
Informações gerais
Neste desafio havia um arquivo de log.txt na qual deveria ser tratado e subido para um banco de dados relacional e a partir dele gerado alguns relatórios.

Arquitetura do projeto
Neste desafio foi separado em: data onde deverá conter os dados de input do projeto, src/ETL onde contem toda a lógica de extração, transformação e carga do projeto, src/utils, onde possui os codigos de ajuda para o ETL no projeto, como por exemplo o handle_json que converte o arquivo original para um arquivo json com padrões de um objeto json, src/database que possui os códigos de conexão com o banco de dados e criação das tabelas, scr/reports, que possui o código de geração dos reports do banco de dados e output onde contém os arquivos de outputs dos relatórios.

Tecnologias
Project is created with:

Python 3.8
MySQL 8.0
numpy
pandas
sqlalchemy
Como utilizar o projeto
Deve ser criado uma pasta data e colocar o arquivo logs.txt dentro da pasta (devido ao tamanho não deu para enviar para o github), após isso, iniciar o servidor do MySQL e criar um banco de dados com o nome "log_project", depois será executar o arquivo src/main.py que chama todos os outros arquivos e executa tudo de uma vez, ou executar arquivo por arquivo para testar cada arquivo separadamente (caso for executar arquivo por arquivo, executar primeiro o arquivo src/utils/handle_json.py para os outros arquivos encontrar o arquivo .json
