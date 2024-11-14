from sqlalchemy import create_engine, Column, String, Integer, DateTime
from sqlalchemy.orm import declarative_base, sessionmaker
from urllib.parse import quote_plus

# SQLAlchemy base model
Base = declarative_base()

class ValidationResult(Base):
    __tablename__ = 'validation_results'
    
    id = Column(Integer, primary_key=True)
    result_type = Column(String)  # 'Source', 'Target', or 'Discrepancy'
    client_id = Column(String)
    account_number = Column(String)
    bank_account_id = Column(String)
    active_flag = Column(String)
    update_datetime = Column(DateTime)

driver = "ODBC Driver 17 for SQL Server"
server = "SL-T600019626L2"

def get_connection_string(database):
    """Construct the connection string for the database."""
    return f"mssql+pyodbc://{server}/{database}?driver={quote_plus(driver)}&trusted_connection=yes"

def get_engine(database):
    """Create a SQLAlchemy engine for the specified database."""
    connection_string = get_connection_string(database)
    return create_engine(connection_string)

# Database engine
engine = get_engine("ETL_TestDB")  # Change to your actual database name
Session = sessionmaker(bind=engine)
