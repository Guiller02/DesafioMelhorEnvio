from sqlalchemy.orm import relationship
from sqlalchemy import create_engine, Table, Column, MetaData, Boolean, Integer, Unicode, String, ForeignKey, Date


class Database:
    engine = ''
    metadata = ''

    def __init__(self):
        self.engine = create_engine('mysql://guiller:1234@localhost/LOG_PROJECT', echo=True)
        self.metadata = MetaData(bind=self.engine)

    def create_tables(self):
        print('Creting Databases!')
        # TB_HOSTS_REQUESTS_DIM

        tb_requests = Table('TB_REQUESTS_DIM',
                            self.metadata,
                            Column('request_id', Integer, primary_key=True),
                            Column('host_requests', String(200)),
                            Column('url_requests', String(200)),
                            )
        print('TB_REQUESTS_DIM created or already was created!')

        tb_routes = Table('TB_ROUTES_DIM',
                          self.metadata,
                          Column('route_id', Integer, primary_key=True),
                          Column('hosts_route', String(200)),
                          )
        print('TB_ROUTES_DIM created or already was created!')

        tb_requests = Table('TB_SERVICES_DIM',
                            self.metadata,
                            Column('service_id', Integer, primary_key=True),
                            Column('host_service', String(200)),
                            Column('name_service', String(200)),
                            )
        print('TB_SERVICES_DIM created or already was created!')

        tb_log = Table('TB_LOG_FACT', self.metadata,
                       Column('LOG_ID', Integer, primary_key=True),
                       Column('authenticated_entity_log', String(200)),
                       Column('started_at_log', Date),
                       Column('status_response', Integer),
                       Column('proxy_latencies', Integer),
                       Column('kong_latencies', Integer),
                       Column('request_latencies', Integer),
                       Column('request_id', Integer, ForeignKey('TB_REQUESTS_DIM.request_id', ondelete='CASCADE')),
                       Column('route_id', Integer, ForeignKey('TB_ROUTES_DIM.route_id', ondelete='CASCADE')),
                       Column('service_id', Integer, ForeignKey('TB_SERVICES_DIM.service_id', ondelete='CASCADE')),
                       )
        print('TB_LOG_FACT created!')

        self.metadata.create_all()


if __name__ == '__main__':
    database = Database()
    database.create_tables()
