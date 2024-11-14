import pandas as pd
from sqlalchemy import create_engine, text
import os

# Define the connection parameters
driver = "ODBC Driver 17 for SQL Server"
server = "SL-T600019626L2"

def get_connection_string(database):
    """Construct the connection string for the database."""
    return f"mssql+pyodbc://{server}/{database}?driver={driver}&trusted_connection=yes"

def get_engine(database):
    """Create a SQLAlchemy engine for the specified database."""
    connection_string = get_connection_string(database)
    return create_engine(connection_string)

def fetch_data(query, database):
    """Fetch data from the specified database using the given query."""
    engine = get_engine(database)
    with engine.connect() as connection:
        result = connection.execute(text(query))
        data = result.fetchall()
        columns = result.keys()
    return pd.DataFrame(data, columns=columns)

def validate_accounts():
    """Validate account data between source and target databases."""
    query = """
    SELECT
        acc.account_number,
        acc.account_status,
        notes.note_type,
        notes.updated_by,
        ext.designation
    FROM
        accounts AS acc
    JOIN
        AccountNotes AS notes ON acc.account_number = notes.account_number
    JOIN
        AccountExternal AS ext ON acc.account_number = ext.account_number
    """
    
    # Fetch data from source and target databases
    source_data = fetch_data(query, "ETL_TestDB")
    target_data = fetch_data(query, "ETL_TargetDB")

    # Check fetched data
    print("Source Data Count:", len(source_data))
    print("Target Data Count:", len(target_data))

    # If no data fetched, exit early
    if source_data.empty or target_data.empty:
        print("No data fetched from one or both databases. Check your queries.")
        return None, None, None

    # Process data types
    for col in ['account_number', 'account_status', 'note_type', 'updated_by', 'designation']:
        source_data[col] = source_data[col].astype(str)
        target_data[col] = target_data[col].astype(str)

    # Validate data
    validations = {
        'account_number': (source_data['account_number'].str.len() == 6),
        'account_status': (source_data['account_status'] == 'Active'),
        'note_type': (source_data['note_type'].notnull()),
        'updated_by': (source_data['updated_by'].notnull()),
        'designation': (source_data['designation'].notnull())
    }

    source_valid = source_data[validations['account_number'] & validations['account_status'] & 
                                validations['note_type'] & validations['updated_by'] & 
                                validations['designation']]
    
    target_valid = target_data[validations['account_number'] & validations['account_status'] & 
                                validations['note_type'] & validations['updated_by'] & 
                                validations['designation']]

    # Debug output
    print("Source Valid Data Count:", len(source_valid))
    print("Target Valid Data Count:", len(target_valid))

    # Check for discrepancies between source and target
    discrepancies = pd.merge(source_valid, target_valid, 
                              on=['account_number'], 
                              how='outer', 
                              suffixes=('_source', '_target'), 
                              indicator=True)

    # Identify discrepancies in all relevant fields
    discrepancies = discrepancies[
        (discrepancies['_merge'] != 'both') | 
        (discrepancies['account_status_source'] != discrepancies['account_status_target']) | 
        (discrepancies['note_type_source'] != discrepancies['note_type_target']) | 
        (discrepancies['updated_by_source'] != discrepancies['updated_by_target']) | 
        (discrepancies['designation_source'] != discrepancies['designation_target'])
    ]

    # Check the discrepancies DataFrame
    print("Discrepancies Count:", len(discrepancies))
    print("Discrepancies Data:\n", discrepancies)

    # Specify the output directory
    output_dir = os.path.dirname(os.path.abspath(__file__))
    source_valid.to_csv(os.path.join(output_dir, 'source_valid.csv'), index=False)
    target_valid.to_csv(os.path.join(output_dir, 'target_valid.csv'), index=False)
    discrepancies.to_csv(os.path.join(output_dir, 'discrepancies.csv'), index=False)

    # Create summary DataFrame
    summary = pd.DataFrame({
        'Metric': [
            'Source Valid Data Count', 
            'Target Valid Data Count', 
            'Discrepancies Count'
        ],
        'Count': [
            len(source_valid), 
            len(target_valid), 
            len(discrepancies)
        ]
    })

    # Write summary to CSV
    summary_path = os.path.join(output_dir, 'validation_summary.csv')
    summary.to_csv(summary_path, index=False)
    print(f"Validation summary written to {summary_path}.")

    return source_valid, target_valid, discrepancies
