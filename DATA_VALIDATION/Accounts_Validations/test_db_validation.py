import pytest
import pandas as pd
from DATA_VALIDATION.Client_Accounts_Validations.data_validation import validate_accounts

@pytest.fixture
def validated_data():
    """Fixture to fetch validated account data."""
    return validate_accounts()

@pytest.mark.parametrize("column", [
    'account_number', 'account_status', 'note_type', 
    'updated_by', 'designation'
])
def test_column_values_validation(validated_data, column):
    source_valid, target_valid, _ = validated_data
    assert source_valid is not None and target_valid is not None, "Data validation failed due to previous errors."
    
    if column == 'account_number':
        assert all(source_valid['account_number'].str.len() == 6), "account_number in source must be exactly 10 characters."
    elif column == 'account_status':
        assert all(source_valid['account_status'] == 'Active'), "account_status in source is not 'Active'."
    elif column == 'note_type':
        assert all(source_valid['note_type'].notnull()), "note_type in source cannot be null."
    elif column == 'updated_by':
        assert all(source_valid['updated_by'].notnull()), "updated_by in source cannot be null."
    elif column == 'designation':
        assert all(source_valid['designation'].notnull()), "designation in source cannot be null."

def test_row_count_validation(validated_data):
    source_valid, target_valid, _ = validated_data
    assert source_valid is not None and target_valid is not None, "Data validation failed due to previous errors."
    assert len(source_valid) == len(target_valid), "Row count mismatch between source and target"

def test_discrepancies_validation(validated_data):
    _, _, discrepancies = validated_data
    assert discrepancies is not None, "Data validation failed due to previous errors."
    assert discrepancies.empty, "Discrepancies found between source and target data"

def test_validate_accounts(validated_data):
    source_valid, target_valid, discrepancies = validated_data
    
    assert source_valid is not None, "Source valid data is None"
    assert target_valid is not None, "Target valid data is None"
    assert discrepancies is not None, "Discrepancies data is None"

    print("Source Valid Data Count:", len(source_valid))
    print("Target Valid Data Count:", len(target_valid))
    print("Discrepancies Count:", len(discrepancies))
