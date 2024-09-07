import polars as pl
from dagster_duckdb import DuckDBResource
from dagster import EnvVar

class CSV_Resource():
    def __init__(self, path):
        self.path = path

    def load_dataset(self) -> pl.DataFrame:
        return pl.read_csv(self.path)

csv_path = "dagster_quickstart/data/sourcedata/people100.csv"
people_resource = CSV_Resource(path=csv_path)

database_resource = DuckDBResource(
    database=EnvVar("DUCKDB_DATABASE")
)



    
