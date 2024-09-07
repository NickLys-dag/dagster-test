from dagster import Definitions, load_assets_from_modules
from . import assets
from dagster_quickstart.recources import people_resource, database_resource
from dagster_quickstart.jobs import people_delivery_job

all_assets = load_assets_from_modules([assets])
all_jobs = [people_delivery_job]

defs = Definitions(
    assets=all_assets,
    
    resources={"people_csv":people_resource,
               "database": database_resource},
    jobs = all_jobs
)
