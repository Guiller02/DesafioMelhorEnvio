from sqlalchemy.sql import select
import pandas as pd


class Reports:
    _engine = ''
    _report_consumer_request_DF = ''
    _report_service_request_DF = ''
    _report_service_latency_DF = ''
    _folder_output = ''

    def __init__(self, engine, folder_output) -> None:
        print('Starting to generate reports')
        self._engine = engine
        self._folder_output = folder_output

    def generate_reports(self):
        print('Executing querys')
        self._report_consumer_request_DF = pd.DataFrame(self._generate_report_consumer_request(),
                                                        columns=['authenticated_entity_log', 'host_requests'])

        self._report_service_request_DF = pd.DataFrame(self._generate_report_service_request(),
                                                       columns=['name_service', 'host_requests', 'url_requests'])

        self._report_service_latency_DF = pd.DataFrame(self._generate_report_service_latency(),
                                                       columns=['name_service', 'avg_proxy_latencies', 'kong_latencies',
                                                                'request_latencies'])
        print('generating file output')
        self._generate_csv_file()

    def _generate_report_consumer_request(self) -> list:
        report_consumer_request = []
        with self._engine.connect() as con:
            rs = con.execute("""
                select 
                    tlf.authenticated_entity_log 
                    ,GROUP_CONCAT(trd.host_requests ORDER BY tlf.authenticated_entity_log  DESC) AS host_requests
                FROM log_project.tb_log_fact tlf
                join log_project.tb_requests_dim trd 
                on trd.request_id = tlf.request_id 
                group by tlf.authenticated_entity_log 
            """)
            for row in rs:
                report_consumer_request.append(row)
        return report_consumer_request

    # Não entendi o que seria o request nessa situação
    def _generate_report_service_request(self) -> list:
        report_service_request = []
        with self._engine.connect() as con:
            rs = con.execute("""
                SELECT 
                    ds.name_service
                    , rd.host_requests
                    , rd.url_requests
                FROM log_project.TB_LOG_FACT as f
                join log_project.TB_REQUESTS_DIM as rd
                on f.request_id = rd.request_id
                join log_project.TB_SERVICES_DIM as ds
                on ds.service_id = f.service_id
                group by ds.name_service
            """)
            for row in rs:
                report_service_request.append(row)
        return report_service_request

    def _generate_report_service_latency(self) -> list:
        report_service_latency = []

        with self._engine.connect() as con:
            rs = con.execute("""
                SELECT
                    tsd.name_service
                    ,AVG(tlf.proxy_latencies)
                    ,AVG(tlf.kong_latencies)
                    ,AVG(tlf.request_latencies)
                FROM log_project.tb_log_fact tlf
                join log_project.tb_services_dim tsd 
                on tsd.service_id = tlf.service_id
                group by tsd.name_service
            """)
            for row in rs:
                report_service_latency.append(row)
        return report_service_latency

    def _generate_csv_file(self):
        self._report_consumer_request_DF.to_csv(self._folder_output+'report_consumer_request_DF.csv', sep=';', index=False)

        self._report_service_request_DF.to_csv(self._folder_output+'report_service_request_DF.csv', sep=';', index=False)

        self._report_service_latency_DF.to_csv(self._folder_output+'report_service_latency_DF.csv', sep=';', index=False)
        # self.


if __name__ == '__main__':
    from src.ETL import transform
    from src.ETL import extract

    from src.db import database

    db = database.Database()
    db.create_tables()

    reports = Reports(db.engine, '../../output/')
    reports.generate_reports()
