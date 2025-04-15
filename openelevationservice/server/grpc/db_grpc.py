from sqlalchemy import create_engine, Column, Integer
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from openelevationservice import SETTINGS
from geoalchemy2 import Raster
from openelevationservice.server.utils import logger

Base = declarative_base()

db_config = SETTINGS['provider_parameters']
db_url = f"postgresql://{db_config['user_name']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['db_name']}"

class Database:
    def __init__(self, db_url):
        self.db_url = db_url
        self.engine = create_engine(self.db_url)
        sm = sessionmaker(bind=self.engine)
        self.session = sm()

    def get_session(self):
        return self.session
    
    def renew_session(self):
        self.session.close()
        sm = sessionmaker(bind=self.engine)
        self.session = sm()

log = logger.get_logger(__name__)
table_name = SETTINGS['provider_parameters']['table_name']

class Cgiar(Base):
    """Database model for SRTM v4.1 aka CGIAR dataset."""
    
    __tablename__ = table_name
    
    rid = Column(Integer, primary_key=True)
    rast = Column(Raster)
    
    def __repr__(self):
        return '<rid {}, rast {}>'.format(self.rid, self.rast)
    
# Init database
db = Database(db_url)

