from sqlalchemy.orm import Session
import pandas as pd
import numpy as np


class Load:
    _engine = ''
    _metadata = ''
    _request_DF = ''
    _routes_DF = ''
    _services_DF = ''
    _log_DF = ''

    # (db.engine, db.metadata, TS.request_DF, TS.routes_DF, TS.services_DF, TS.log_DF)
    def __init__(self, engine, metadata, requests_DF, routes_DF, services_DF, log_DF):
        print('Starting Load step')
        self._engine = engine
        self._metadata = metadata
        self._request_DF = requests_DF
        self._routes_DF = routes_DF
        self._services_DF = services_DF
        self._log_DF = log_DF

        # Truncate todas as tabelas
        print('Truncating all tables')
        with self._engine.connect() as con:
            con.execute('delete from log_project.tb_log_fact')
            con.execute('delete from log_project.TB_REQUESTS_DIM')
            con.execute('delete from log_project.TB_ROUTES_DIM')
            con.execute('delete from log_project.TB_SERVICES_DIM')

    # Metodo para chamar os outros metodos e inserir os valores nas tabelas
    def inserting_into_tables(self):
        self.insert_requests_data()

        self.insert_routes_data()

        self.insert_services_data()

        # self.insert_log_data()

        self.insert_log_data()

        print('Finished load step')

    # tb_requests_dim
    def insert_requests_data(self):
        print('Inserting into tb_requests_dim table')
        self._request_DF.to_sql('tb_requests_dim', con=self._engine, index=False, if_exists='append')

    # tb_routes_dim
    def insert_routes_data(self):
        print('Inserting into tb_routes_dim table')
        self._routes_DF.to_sql('tb_routes_dim', con=self._engine, index=False, if_exists='append')

    def insert_services_data(self):
        print('Inserting into tb_routes_dim table')
        self._services_DF.to_sql('tb_services_dim', con=self._engine, index=False, if_exists='append')

    s = Session()

    # tb_log_fact

    # Caso remova o chunk dará erro, aqui no meu computador apenas rodou com method='multi' e chunksize=5000
    def insert_log_data(self):
        print('Inserting into tb_log_fact table')
        self._log_DF.to_sql('tb_log_fact', con=self._engine, index=False, if_exists='append', method='multi',
                            chunksize=5000)


# Para testar o arquivo
if __name__ == '__main__':
    from src.ETL import transform
    from src.ETL import extract
    from src.db import database

    db = database.Database()
    db.create_tables()

    EX = extract.Extract('../../data/logs.json')
    EX.create_dataframe()
    TS = transform.Transform(EX.DF)
    TS.transform_data()

    load = Load(db.engine, db.metadata, TS.request_DF, TS.routes_DF, TS.services_DF, TS.log_DF)

    load.inserting_into_tables()
