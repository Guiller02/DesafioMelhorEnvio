# Imports
import numpy as np
import pandas as pd
import datetime

# Configurar o pandas para não mostrar um limite de colunas, e não mostrar warning nas transformações que estão
# fazendo cópia do dataframe e criando outro por cima
pd.options.display.max_columns = 500
pd.options.mode.chained_assignment = None


class Transform:
    _DF = ''
    request_DF = ''
    routes_DF = ''
    services_DF = ''
    log_DF = ''

    def __init__(self: classmethod, dataframe: dict) -> None:
        # Ao chamar a classe, enviar o dataframe, e converter tudo para string, para não dar erro em quando for
        # separar ou dividir os dados
        print('Starting the transformation step')
        self._DF = dataframe.applymap(str)

    # To insert id in the first position
    def change_columns_positions(self: classmethod) -> None:
        self.log_DF = self.log_DF[
            ['log_id','authenticated_entity_log', 'started_at_log', 'status_response','proxy_latencies', 'kong_latencies', 'request_latencies', 'request_id', 'service_id',
             'route_id']]
        self.request_DF = self.request_DF[['request_id','host_requests','url_requests' ]]

        self.routes_DF = self.routes_DF[['route_id', 'hosts_route']]

        self.services_DF = self.services_DF[['service_id', 'host_service', 'name_service']]

    # Metodo principal, que chama os outros metodos e trata os dados
    def transform_data(self: classmethod) -> None:
        # criar um index de log, que será utilizado depois para fazer os joins dos dados
        self._DF["log_id"] = self._DF.index + 1
        print('Separating dataframes')
        self._separate_requests()
        self._separate_routes()
        self._separate_services()
        self._separate_log()
        print('Normalizing data')

        self._merge_log_df()
        self.change_columns_positions()

        print('Finished transformation step')

    # separação dos dados que são relacionados a coluna de requests
    def _separate_requests(self: classmethod) -> None:
        self.request_DF = self._DF[['host_requests', 'url_requests']]

        self.request_DF = self.request_DF.drop_duplicates(subset='host_requests')

        self.request_DF.reset_index(drop=True)

        self.request_DF["request_id"] = self.request_DF.index + 1

    # separação dos dados que são relacionados a coluna de routes
    def _separate_routes(self: classmethod) -> None:
        self.routes_DF = self._DF[['hosts_route']]

        self.routes_DF = self.routes_DF.drop_duplicates(subset='hosts_route')

        self.routes_DF.reset_index(drop=True)

        self.routes_DF["route_id"] = self.routes_DF.index + 1

    # separação dos dados que são relacionados a coluna de services
    def _separate_services(self: classmethod) -> None:
        self.services_DF = self._DF[['host_service', 'name_service']]

        self.services_DF = self.services_DF.drop_duplicates(subset='name_service')

        self.services_DF.reset_index(drop=True)

        self.services_DF["service_id"] = self.services_DF.index + 1

    # separação dos dados que são relacionados a coluna de log (que será a tabela fato)
    def _separate_log(self):
        self.log_DF = self._DF[
            ['upstream_uri_log', 'authenticated_entity_log', 'started_at_log', 'log_id',
             'status_response', 'size_response', 'proxy_latencies', 'kong_latencies', 'request_latencies',
             'host_requests', 'hosts_route', 'name_service']]

        self.log_DF['started_at_log'] = self.log_DF['started_at_log'].astype(np.int64).apply(
            lambda x: datetime.datetime.fromtimestamp(x))

    # Esse metodo pega todos os dataframes que possuem a coluna log_id e faz o merge para o dataframe log_DF
    def _merge_log_df(self):
        self.log_DF = pd.merge(self.log_DF, self.request_DF, left_on='host_requests', right_on='host_requests')

        self.log_DF = pd.merge(self.log_DF, self.services_DF, left_on='name_service', right_on='name_service')

        self.log_DF = pd.merge(self.log_DF, self.routes_DF, left_on='hosts_route', right_on='hosts_route')


# Apenas para testar os metodos pelo próprio arquivo
if __name__ == '__main__':
    import extract

    EX = extract.Extract('../../data/logs.json')
    EX.create_dataframe()
    transform = Transform(EX.DF)
    transform.transform_data()

    print('---------Request----------')
    print(transform.request_DF.head())
    print('--------------------------')

    print('---------Rout----------')
    print(transform.routes_DF.head())
    print('--------------------------')

    print('---------services----------')
    print(transform.services_DF.head())
    print('--------------------------')

    print('---------log----------')
    print(transform.log_DF.head())
    print('--------------------------')