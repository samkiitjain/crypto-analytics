import pytest
import pandas as pd
from datetime import date
from utils.file_writer import write_parquet


@pytest.fixture
def sample_dataframe():
    """Create sample dataframe to be written in parquet format."""
    return pd.DataFrame({
        "coin_id":       ["bitcoin"],
        "close_usd":     [67000.0],
        "timestamp_utc": ["2026-04-06T00:00:00Z"],
    })

@pytest.fixture
def output_base(tmp_path):
    """Create a temporary directory for testing file output."""
    return tmp_path / "prices"


##Test cases

def test_write_parquet(sample_dataframe, output_base):
    """Test that the write_parquet function correctly writes a DataFrame to a Parquet file with the expected structure."""
    partition_date = date(2026, 4, 6)
    file_name = "test_bitcoin_price"
    
    # Call the function to write the parquet file
    output_path = write_parquet(sample_dataframe, output_base, partition_date, file_name)
    
    # Check that the file was created at the expected location
    expected_path = output_base / "year=2026" / "month=04" / "day=06" / f"{file_name}.parquet"
    assert output_path == expected_path, f"Expected path {expected_path}, but got {output_path}"
    
    # Check that the file exists
    assert output_path.exists(), f"Expected file at {output_path} does not exist."
    
    # Read the parquet file back into a DataFrame and compare with original
    df_read = pd.read_parquet(output_path)
    pd.testing.assert_frame_equal(df_read, sample_dataframe)

