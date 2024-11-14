import pandas as pd
from ..db_setup import get_engine  # Adjusted import to reflect the package structure

def fetch_data(query, database):
    """Fetch data from the specified database using the given SQL query."""
    engine = get_engine(database)
    with engine.connect() as connection:
        return pd.read_sql(query, connection)
