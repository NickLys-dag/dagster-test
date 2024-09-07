from dagster import asset
from dagster_quickstart.recources import people_resource
from dagster_duckdb import DuckDBResource
import polars as pl
from datetime import date


@asset(group_name="people")
def people():
    """Source file from github"""
    dataframe = people_resource.load_dataset()
    return dataframe

@asset(group_name="people")
def add_working_column(people):
    """Adds a copy of 'Last Name' field"""
    dataframe = pl.DataFrame(people)
    dataframe = dataframe.with_columns([pl.col("Last Name").alias("New_Last_Name")])
    return dataframe

@asset(group_name="people")
def shuffle_last_name(add_working_column):
    """Shuffles the 'last name' field with validatation"""
    dataframe = pl.DataFrame(add_working_column)
    controldf = dataframe.filter(pl.col("Last Name") == pl.col("New_Last_Name"))
    num_rows = controldf.shape[0]

    while num_rows > 0:
        dataframe = dataframe.with_columns([pl.col("New_Last_Name").shuffle()])
        controldf = dataframe.filter(pl.col("Last Name") == pl.col("New_Last_Name"))
        num_rows = controldf.shape[0]
    dataframe = dataframe.with_columns([pl.col("New_Last_Name").alias("Last Name")]).drop("New_Last_Name")
    return dataframe

@asset(group_name="people")
def add_age_average(shuffle_last_name):
    """Adds an average age per Job Title"""
    current_date = date.today()
    dataframe = pl.DataFrame(shuffle_last_name) 
    dataframe = dataframe.with_columns(pl.col("Date of birth").str.strptime(pl.Date, format="%Y-%m-%d").alias("Date of birth"))

    #dataframe = dataframe.with_columns((pl.lit(current_date).dt.year() - pl.col("Date of birth").dt.year()).alias("base_age"))
    # dataframe = dataframe.with_columns(
    #     pl.when(pl.lit(current_date).dt.month() < pl.col("Date of birth").dt.month() | 
    #             ((pl.lit(current_date).dt.month() == pl.col("Date of birth").dt.month()) & (pl.lit(current_date).dt.day() < pl.col("Date of birth").dt.day())))
    #     .then(pl.col("base_age") - 1)
    #     .otherwise(pl.col("base_age"))
    #     .cast(pl.Int64)
    #     .alias("age"))#.drop("base_age")

    dataframe = dataframe.with_columns(
    (   
        pl.lit(current_date).dt.year() - pl.col("Date of birth").dt.year() -  
        (
            (pl.lit(current_date).dt.month() < pl.col("Date of birth").dt.month()) | 
            (
                (pl.lit(current_date).dt.month() == pl.col("Date of birth").dt.month()) &
                (pl.lit(current_date).dt.day() < pl.col("Date of birth").dt.day())
            )          
        ).cast(pl.Int64)).alias("age")
    )
    dataframe = dataframe.with_columns(pl.col("age").mean().over("Job Title").alias("avg_age")).drop("age")
    return dataframe

@asset(group_name="people")
def people_db(database: DuckDBResource, add_age_average)-> None:
    """Target delivery to duckDB """
    dataframe = pl.DataFrame(add_age_average)

    query = f"""
      create or replace table people as (
        select
          Index,
          "User Id",
          "First Name",
          "Last Name",
          Sex,
          Email,
          Phone,
          "Date of birth",
          "Job Title",
          avg_age
        from 'people'
      );
    """
    with database.get_connection() as conn:
        conn.register("people", dataframe)
        conn.execute(query)
