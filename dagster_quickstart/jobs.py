from dagster import job, AssetSelection, define_asset_job

people_assets = AssetSelection.assets("people",
                                      "add_working_column",
                                      "shuffle_last_name",
                                      "add_age_average",
                                      "people_db")

people_delivery_job = define_asset_job(
    name="people_delivery_job",
    selection=people_assets,
)