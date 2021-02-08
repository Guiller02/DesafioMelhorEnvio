# imports
import json
import pandas as pd

# Criar a classe Extract, para criar um dataframe contendo os dados do arquivo json
class Extract:
    _jsonFile = ''
    _json_data = ''
    DF = ''

    # Criando arrays vazios para dar append com o conteudo do arquivo json

    # log
    _upstream_uri_log = []
    _authenticated_entity_log = []
    _client_ip_log = []
    _started_at_log = []

    # requests
    _urls_requests = []
    _sizes_request = []
    _querystrings_requests = []
    _accepts_requests = []
    _hosts_requests = []
    _user_agents_requests = []

    # responses
    _status_response = []
    _sizes_response = []
    _contents_Lengths_response = []
    _vias_response = []
    _connections_response = []
    _access_control_allow_credentials_response = []
    _content_types_response = []
    _servers_response = []
    _access_control_allow_origin_response = []

    # routes
    _created_at_route = []
    _hosts_route = []
    _id_route = []
    _preserve_host_route = []
    _regex_priority_route = []
    _service_route = []
    _strip_path_route = []
    _updated_at_route = []

    _methods_route = []

    _paths_route = []

    _protocols_route = []

    # services
    _connect_timeout_service = []
    _created_at_service = []
    _host_service = []
    _id_service = []
    _name_service = []
    _path_service = []
    _port_service = []
    _protocol_service = []
    _read_timeout_service = []
    _retries_service = []
    _updated_at_service = []
    _write_timeout_service = []

    # Latencies
    _proxy_latencies = []
    _kong_latencies = []
    _request_latencies = []

    # Quando for chamado a classe Extract, inserir o arquivo json que será lido e chamar o metodo que lê o arquivo json
    def __init__(self: classmethod, json_file: str) -> None:
        self._jsonFile = json_file
        self._read_json()

    # Lendo o arquivo json criado em utils.handle_json
    def _read_json(self: classmethod) -> None:
        print('starting extract step, reading: ' + self._jsonFile)

        with open(self._jsonFile, 'r') as f:
            data = f.read()

            self._json_data = json.loads(data)
        print('finished reading')

    # Apenas para testar os dados do dataframe
    def test_json_columns(self: classmethod) -> None:
        print('--------------------------')
        print(json.dumps(self._json_data[0], indent=4))
        print('--------------------------')

    # Esse metodo serve para dar append nos arrays criados acima
    def _append_arrays(self: classmethod):
        for _ in self._json_data:
            # log
            self._upstream_uri_log.append(_['upstream_uri'])
            self._authenticated_entity_log.append(_['authenticated_entity']['consumer_id']['uuid'])
            self._client_ip_log.append(_['client_ip'])
            self._started_at_log.append(_['started_at'])

            # requests
            self._urls_requests.append(_['request']['url'])
            self._sizes_request.append(_['request']['size'])
            self._querystrings_requests.append(_['request']['querystring'])

            self._accepts_requests.append(_['request']['headers']['accept'])
            self._hosts_requests.append(_['request']['headers']['host'])
            self._user_agents_requests.append(_['request']['headers']['user-agent'])

            # responeses
            self._status_response.append(_['response']['status'])
            self._sizes_response.append(_['response']['size'])

            self._contents_Lengths_response.append(_['response']['headers']['Content-Length'])
            self._vias_response.append(_['response']['headers']['via'])
            self._connections_response.append(_['response']['headers']['Connection'])
            self._access_control_allow_credentials_response.append(
                _['response']['headers']['access-control-allow-credentials'])
            self._content_types_response.append(_['response']['headers']['Content-Type'])
            self._servers_response.append(_['response']['headers']['server'])
            self._access_control_allow_origin_response.append(_['response']['headers']['access-control-allow-origin'])

            # routes
            self._created_at_route.append(_['route']['created_at'])
            self._hosts_route.append(_['route']['hosts'])
            self._id_route.append(_['route']['id'])
            self._preserve_host_route.append(_['route']['preserve_host'])
            self._regex_priority_route.append(_['route']['regex_priority'])
            self._service_route.append(_['route']['service']['id'])
            self._strip_path_route.append(_['route']['strip_path'])
            self._updated_at_route.append(_['route']['updated_at'])

            # Para não dar erro, criei passei todos os dados que eram de array para string, para tratá-los em transform
            # (porém acabei não utilizando em transform)
            self._methods_route.append(','.join(str(_) for _ in _['route']['methods']))

            self._paths_route.append(','.join(str(_) for _ in _['route']['paths']))

            self._protocols_route.append(','.join(str(_) for _ in _['route']['protocols']))

            # services
            self._connect_timeout_service.append(_['service']['connect_timeout'])
            self._created_at_service.append(_['service']['created_at'])
            self._host_service.append(_['service']['host'])
            self._id_service.append(_['service']['id'])
            self._name_service.append(_['service']['name'])
            self._path_service.append(_['service']['path'])
            self._port_service.append(_['service']['port'])
            self._protocol_service.append(_['service']['protocol'])
            self._read_timeout_service.append(_['service']['read_timeout'])
            self._retries_service.append(_['service']['retries'])
            self._updated_at_service.append(_['service']['updated_at'])
            self._write_timeout_service.append(_['service']['write_timeout'])

            # latencies
            self._proxy_latencies.append(_['latencies']['proxy'])
            self._kong_latencies.append(_['latencies']['kong'])
            self._request_latencies.append(_['latencies']['request'])

    # Metodo para criar e retornar o dicionário que será utilizado para criar o dataframe
    def _create_dict(self: classmethod) -> dict:
        self._append_arrays()

        log_dict = {
            # log
            'upstream_uri_log': self._upstream_uri_log,
            'authenticated_entity_log': self._authenticated_entity_log,
            'client_ip_log': self._client_ip_log,
            'started_at_log': self._started_at_log,

            # requests
            'url_requests': self._urls_requests,
            'size_request': self._sizes_request,
            'querystring_requests': self._querystrings_requests,
            'accept_requests': self._accepts_requests,
            'host_requests': self._hosts_requests,
            'user_agent_requests': self._user_agents_requests,

            # response
            'status_response': self._status_response,
            'size_response': self._sizes_response,
            '_content_Length_response': self._contents_Lengths_response,
            'via_response': self._vias_response,
            'connection_response': self._connections_response,
            '_access_control_allow_credentials_response': self._access_control_allow_credentials_response,
            '_content_type_response': self._content_types_response,
            'server_response': self._servers_response,
            '_access_control_allow_origin_response': self._access_control_allow_origin_response,

            # routes
            'created_at_route': self._created_at_route,
            'hosts_route': self._hosts_route,
            'id_route': self._id_route,
            'preserve_host_route': self._preserve_host_route,
            'regex_priority_route': self._regex_priority_route,
            'service_route': self._service_route,
            'strip_path_route': self._strip_path_route,
            'updated_at_route': self._updated_at_route,
            'methods_route': self._methods_route,
            'paths_route': self._paths_route,
            'protocols_route': self._protocols_route,

            # services
            'connect_timeout_service': self._connect_timeout_service,
            'created_at_service': self._created_at_service,
            'host_service': self._host_service,
            'id_service': self._id_service,
            'name_service': self._name_service,
            'path_service': self._path_service,
            'port_service': self._port_service,
            'protocol_service': self._protocol_service,
            'read_timeout_service': self._read_timeout_service,
            'retries_service': self._retries_service,
            'updated_at_service': self._updated_at_service,
            'write_timeout_service': self._write_timeout_service,

            # latencies
            'proxy_latencies': self._proxy_latencies,
            'kong_latencies': self._kong_latencies,
            'request_latencies': self._request_latencies,

        }

        return log_dict

    # Metodo para criar o dataframe
    def create_dataframe(self: classmethod) -> None:
        print('Creating dataframe')

        # Criar o dataframe utilizando o return do metodo
        self.DF = pd.DataFrame(self._create_dict())

        print('finished extract step')


# Para testar o arquivo
if __name__ == '__main__':
    extract = Extract('../../data/logs.json')

    extract.test_json_columns()

    extract.create_dataframe()

    print(extract.DF.head())

    print(extract.DF.dtypes)