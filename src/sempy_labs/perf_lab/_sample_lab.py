import pandas as pd
import time
import notebookutils

from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, last_day, dayofweek, year, month, date_format, rand, randn, expr
from typing import Optional, Tuple, Callable
from datetime import date, datetime
from dateutil.relativedelta import relativedelta
from uuid import UUID

import sempy.fabric as fabric
import sempy_labs._icons as icons
from sempy.fabric.exceptions import WorkspaceNotFoundException, FabricHTTPException
from sempy_labs import migration, directlake, admin
from sempy_labs import lakehouse as lh

from sempy_labs import refresh_semantic_model
from sempy_labs.tom import connect_semantic_model
from sempy_labs._helper_functions import (
    resolve_workspace_name_and_id,
    get_capacity_id,
    resolve_lakehouse_name_and_id,
    resolve_dataset_name_and_id,
    create_abfss_path,
)

class PropertyBag:
    def __init__(self):
        self._properties = {}

    def add_property(self, key, value):
        self._properties[key] = value

    def remove_property(self, key):
        if key in self._properties:
            del self._properties[key]

    def get_property(self, key):
        return self._properties.get(key, None)

    def has_property(self, key):
        return key in self._properties

    def __str__(self):
        return str(self._properties)


def _get_or_create_workspace(
    workspace: Optional[str | UUID] = None,
    capacity_id: Optional[UUID] = None,
    description: Optional[str] = None,
) -> Tuple[str, UUID]:
    """
    Creates a workspace on a Fabric capacity to host perf lab resources.

    Parameters
    ----------
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    capacity_id : uuid.UUID, default=None
        The ID of the capacity on which to place the new workspace.
        Defaults to None which resolves to the capacity of the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the capacity of the workspace of the notebook.
    description : str, default=None
        The optional description of the workspace.
        Defaults to None which leaves the description blank.    
    Returns
    -------
    Tuple[str, UUID]
        A tuple holding the name and ID of the workspace.
    """

    try:
        # If the workspace already exist, return the resolved name and ID.
        (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
        print(f"{icons.green_dot} Workspace '{workspace_name}' already exists. Skipping workspace creation.")
        return (workspace_name,workspace_id)

    except WorkspaceNotFoundException:
        # Otherwise create a new workspace.
        try:
            # But only if a human-friendly name was provided. If it's a Guid, raise an exception.
            UUID(workspace)
            raise ValueError("For new workspaces, the workspace parameter must be string, not a Guid. Please provide a workspace name.")
        except ValueError:
            # OK, it's not a Guid. But also make sure the workspace parameter isn't empty.
            if workspace == "" or workspace is None:
                raise ValueError("For new workspaces, the workspace parameter cannot be None or empty. Please provide a workspace name.")

        # Get the capacity id from the attached lakehouse or notebook workspace if no ID was provided.
        if capacity_id is None:
            capacity_id = get_capacity_id()

        # Provision the new workspace and return the workspace info.
        workspace_id = fabric.create_workspace(display_name=workspace, capacity_id=capacity_id, description=description)
        (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace_id)
        print(f"{icons.green_dot} Workspace '{workspace_name}' created.")
        return (workspace,workspace_id)
    
def _get_or_create_lakehouse(
    lakehouse: Optional[str | UUID] = None,
    workspace: Optional[str | UUID] = None,
    description: Optional[str] = None,
) -> Tuple[str, UUID]:
    """
    Creates or retrieves a Fabric lakehouse.

    Parameters
    ----------
    lakehouse : str | uuid.UUID, default=None
        The name or ID of the lakehouse.
        Defaults to None which resolves to the lakehouse attached to the notebook.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID where the lakehouse is located.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    description : str, default=None
        The optional description for the lakehouse.
        Defaults to None which leaves the description blank.    
    Returns
    -------
    Tuple[str, UUID]
        A tuple holding the name and ID of the lakehouse.
    """

    # Treat empty strings as None.
    if lakehouse == "":
       lakehouse = None 
    if workspace == "":
       workspace = None 
    
    # Make sure the workspace exists. Raises WorkspaceNotFoundException otherwise.
    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)

    try:
        # Raises a ValueError if there's no lakehouse with the specified name in the workspace.
        (lakehouse_name, lakehouse_id) = resolve_lakehouse_name_and_id(
            lakehouse=lakehouse, workspace=workspace_id
        )
        
        # Otherwise, return the name and id of the existing lakehouse.
        print(f"{icons.green_dot} Lakehouse '{lakehouse_name}' already exists. Skipping lakehouse creation.")
        return (lakehouse_name, lakehouse_id)
    except ValueError:
        # If there is no existing lakehouse, check that the lakehouse name is valid so that we can create one.
        try:
            # But only if a human-friendly name was provided. If it's a Guid, raise an exception.
            UUID(lakehouse)
            raise ValueError("For new lakehouses, the lakehouse parameter must be string, not a Guid. Please provide a lakehouse name.")
        except:
            # OK, it's not a Guid. Also check that the lakehouse name is not blank.
            if lakehouse is None:
                raise ValueError("For new lakehouses, the lakehouse parameter cannot be None or empty. Please provide a lakehouse name.")

    lakehouse_id = fabric.create_lakehouse(display_name=lakehouse, workspace=workspace_id, description=description)
    (lakehouse_name, lakehouse_id) = resolve_lakehouse_name_and_id(
            lakehouse=lakehouse_id, workspace=workspace_id )
    print(f"{icons.green_dot} Lakehouse '{lakehouse_name}' created.")
    return (lakehouse, lakehouse_id)

def _get_product_categories_df() -> DataFrame:
    """
    Generates sample data for a productcategory table.

    Returns
    -------
    DataFrame
        A Spark dataframe with a small list of product categories.
    """
    spark = SparkSession.builder \
        .appName("PerfLabGenerator") \
        .getOrCreate()

    # A small dimension table
    data = [(1, 'Accessories'), (2, 'Bikes'), (3, 'Clothing')]
    columns = ['ProductCategoryID', 'ProductCategory']

    return spark.createDataFrame(data, columns)

def _get_dates_df(
    start_date: Optional[str | date] = None,
    years: Optional[int] = 4,
) -> DataFrame:
    """
    Generates sample data for a date table.

    Parameters
    ----------
    start_date : str | date, default=None
        The start date for the date table. If specified as a string, must adhere to the format "%Y-%m-%d", such as "2025-01-25".
        Defaults to None which resolves to the current date minus the specified years.
    years : int, default=4
        The number of years that the date table covers.
        The value must be greater than 0. Defaults to 4.

    Returns
    -------
    DataFrame
        A Spark dataframe with the typical columns of a basic date table.
    """
    #years must be greater than 0.
    if years < 1:
        raise ValueError("Years must be greater than 0.")

    # Make sure the dates are valid
    if start_date is None or start_date == "":
       start_date = date.today() - relativedelta(years=years)
    else:
        if isinstance(start_date, str):
            start_date = datetime.strptime(start_date, "%Y-%m-%d")

    end_date = start_date + relativedelta(years=years)

    # Generate the date table data.
    spark = SparkSession.builder \
        .appName("PerfLabGenerator") \
        .getOrCreate()

    date_df = pd.date_range(start=start_date, end=end_date).to_frame(index=False, name='Date')
    date_df['Date'] = date_df['Date'].astype(str)

    spark_df  = spark.createDataFrame(date_df)
    spark_df = spark_df.withColumn('Date', col('Date').cast('date'))
    spark_df = spark_df.withColumn('DateID', date_format(col('Date'),"yyyyMMdd").cast('integer'))
    spark_df = spark_df.withColumn('Monthly', date_format(col('Date'),"yyyy-MM-01").cast('date'))
    spark_df = spark_df.withColumn('Month', date_format(col('Date'),"MMM"))
    spark_df = spark_df.withColumn('MonthYear', date_format(col('Date'),"MMM yyyy"))
    spark_df = spark_df.withColumn('MonthOfYear', month(col('Date')))
    spark_df = spark_df.withColumn('Year', year(col('Date')))
    spark_df = spark_df.withColumn('EndOfMonth', last_day(col('Date')).cast('date'))
    spark_df = spark_df.withColumn('DayOfWeekNum', dayofweek(col('Date')))
    spark_df = spark_df.withColumn('DayOfWeek', date_format(col('Date'),"EE"))
    spark_df = spark_df.withColumn('WeeklyStartSun', col('Date')+1-dayofweek(col('Date')))
    spark_df = spark_df.withColumn('WeeklyStartMon', col('Date')+2-dayofweek(col('Date')))

    return spark_df 

def _get_measure_table_df() -> DataFrame:
    """
    Generates a tiny table to store the measures in the model

    Returns
    -------
    DataFrame
        A Spark dataframe for the measures table.
    """
    spark = SparkSession.builder \
        .appName("PerfLabGenerator") \
        .getOrCreate()

    # A table to store the measures in the model
    data = [('Measures only',)]
    columns = ['Col1']

    return spark.createDataFrame(data, columns)

def _get_geography_df() -> DataFrame:
    """
    Generates a larger dimension table, with USA and Australia and their states/territories.

    Returns
    -------
    DataFrame
        A Spark dataframe with sample data for a geography table.
    """
    spark = SparkSession.builder \
        .appName("PerfLabGenerator") \
        .getOrCreate()

    # A table with geography sample data
    data = [(1, 'Alabama', 'USA'),(2, 'Alaska', 'USA'),(3, 'Arizona', 'USA'),(4, 'Arkansas', 'USA'),(5, 'California', 'USA'),(6, 'Colorado', 'USA'),(7, 'Connecticut', 'USA'),(8, 'Delaware', 'USA'),(9, 'Florida', 'USA'),(10, 'Georgia', 'USA'),(11, 'Hawaii', 'USA'),(12, 'Idaho', 'USA'),(13, 'Illinois', 'USA'),(14, 'Indiana', 'USA'),(15, 'Iowa', 'USA'),(16, 'Kansas', 'USA'),(17, 'Kentucky', 'USA'),(18, 'Louisiana', 'USA'),(19, 'Maine', 'USA'),(20, 'Maryland', 'USA'),(21, 'Massachusetts', 'USA'),(22, 'Michigan', 'USA'),(23, 'Minnesota', 'USA'),(24, 'Mississippi', 'USA'),(25, 'Missouri', 'USA'),(26, 'Montana', 'USA'),(27, 'Nebraska', 'USA'),(28, 'Nevada', 'USA'),(29, 'New Hampshire', 'USA'),(30, 'New Jersey', 'USA'),(31, 'New Mexico', 'USA'),(32, 'New York', 'USA'),(33, 'North Carolina', 'USA'),(34, 'North Dakota', 'USA'),(35, 'Ohio', 'USA'),(36, 'Oklahoma', 'USA'),(37, 'Oregon', 'USA'),(38, 'Pennsylvania', 'USA'),(39, 'Rhode Island', 'USA'),(40, 'South Carolina', 'USA'),(41, 'South Dakota', 'USA'),(42, 'Tennessee', 'USA'),(43, 'Texas', 'USA'),(44, 'Utah', 'USA'),(45, 'Vermont', 'USA'),(46, 'Virginia', 'USA'),(47, 'Washington', 'USA'),(48, 'West Virginia', 'USA'),(49, 'Wisconsin', 'USA'),(50, 'Wyoming', 'USA'),(51, 'New South Wales', 'Australia'),(52, 'Queensland', 'Australia'),(53, 'South Australia', 'Australia'),(54, 'Tasmania', 'Australia'),(55, 'Victoria', 'Australia'),(56, 'Western Australia', 'Australia'),(57, 'Australian Capital Territory', 'Australia'),(58, 'Northern Territory', 'Australia')]
    columns = ['GeoID', 'StateOrTerritory', 'Country']

    return spark.createDataFrame(data, columns)

def _get_sales_df(
    start_date: Optional[str | date] = None,
    years: Optional[int] = 4,
    num_rows_in_millions: Optional[int] = 100,
    seed: Optional[int] = 1,
) -> DataFrame:
    """
    Generates a fact table with random links to date, product category, and geography dimensions
    and a sales column with random generated numbers (1-1000)
    and a costs column with random generated numbers (1-100)

    Parameters
    ----------
    start_date : str | date, default=None
        The start date for the transactions in the sales table. If specified as a string, must adhere to the format "%Y-%m-%d", such as "2025-01-25".
        Defaults to None which resolves to the current date minus the specified years.
    years : int, default=4
        The number of years that the transactions in the sales table cover.
        The value must be greater than 0. Defaults to 4.
    num_rows_in_millions : int, default=100
        The number of transactions in the sales table in millions.
        The value must be greater than 0. Defaults to 100 for 100 million rows.
    seed: int, default=1
        A seed value for the random numbers generator. Defaults to 1.

    Returns
    -------
    DataFrame
        A Spark dataframe with sample sales data for a fact table.
    """

    #years must be greater than 0.
    if years < 1:
        raise ValueError("Years must be greater than 0.")

    #num_rows_in_millions must be greater than 0.
    if num_rows_in_millions < 1:
        raise ValueError("The number of rows in millions must be greater than 0.")

    # Make sure the start_date is valid
    if start_date is None or start_date == "":
       start_date = date.today() - relativedelta(years=years)
    else:
        if isinstance(start_date, str):
            start_date = datetime.strptime(start_date, "%Y-%m-%d")

    spark = SparkSession.builder \
        .appName("PerfLabGenerator") \
        .getOrCreate()

    if num_rows_in_millions > 10:
        print(f"{icons.in_progress} Generating {num_rows_in_millions} million rows of random sales data.")

    return spark.range(0, num_rows_in_millions * 1000000).withColumn('ProductCategoryID', (rand(seed=seed)*3+1).cast('int')) \
        .withColumn('GeoID', (rand(seed=seed)*58+1).cast('int')) \
        .withColumn('DateID', expr(f'cast(date_format(date_add("{start_date}", cast(rand(100) * 365 * {years} as int)), "yyyyMMdd") as int)')) \
        .withColumn('Sales', (rand(seed=seed*4)*1000+1).cast('int')) \
        .withColumn('Costs', (rand(seed=seed+45)*100+1).cast('int'))


def _save_as_delta_table(
    dataframe: DataFrame,
    delta_table_name: str,
    lakehouse: Optional [str | UUID] = None,
    workspace: Optional [str | UUID] = None,
):
    """
    Saves a spark dataframe as a delta table in a Fabric lakehouse.

    Parameters
    ----------
    dataframe : DataFrame
        The spark dataframe to be saved as a delta table.
    delta_table_name : str
        The name of the delta table.
    lakehouse : uuid.UUID
        The Fabric lakehouse ID.
        Defaults to None which resolves to the lakehouse attached to the notebook.
    workspace : uuid.UUID
        The Fabric workspace ID where the specified lakehouse is located.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    (lakehouse_name, lakehouse_id) = resolve_lakehouse_name_and_id(lakehouse=lakehouse,workspace=workspace_id)

    filePath = create_abfss_path(
        lakehouse_id=lakehouse_id,
        lakehouse_workspace_id=workspace_id,
        delta_table_name=delta_table_name,
    )
    dataframe.write.mode("overwrite").format("delta").save(filePath)
    print(f"{icons.green_dot} Delta table '{delta_table_name}' created and {dataframe.count()} rows inserted.")

def _read_delta_table(
    delta_table_name: str,
    lakehouse: Optional [str | UUID] = None,
    workspace: Optional [str | UUID] = None,
) -> DataFrame:
    """
    Returns a spark dataframe with the rows of a delta table in a Fabric lakehouse.

    Parameters
    ----------
    delta_table_name : str
        The name of the delta table.
    lakehouse : uuid.UUID
        The Fabric lakehouse ID.
        Defaults to None which resolves to the lakehouse attached to the notebook.
    workspace : uuid.UUID
        The Fabric workspace ID where the specified lakehouse is located.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    DataFrame
        A Spark dataframe with the data from the specified delta table.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    (lakehouse_name, lakehouse_id) = resolve_lakehouse_name_and_id(lakehouse=lakehouse,workspace=workspace_id)

    filePath = create_abfss_path(
        lakehouse_id=lakehouse_id,
        lakehouse_workspace_id=workspace_id,
        delta_table_name=delta_table_name,
    )
    spark = SparkSession.builder \
        .appName("PerfLabDeltaTableReader") \
        .getOrCreate()
    
    return spark.read.format("delta").load(filePath)

def provision_perf_lab_lakehouse(
    workspace: Optional[str | UUID] = None,
    capacity_id: Optional[UUID] = None,
    lakehouse: Optional[str | UUID] = None,
    table_properties: Optional[PropertyBag] = None,
    table_generator: Optional[Callable] = None,
)  -> Tuple[UUID, UUID]:
    """
    Generates sample data for a date table.

    Parameters
    ----------
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    capacity_id : uuid.UUID, default=None
        The ID of the capacity on which to place the new workspace.
        Defaults to None which resolves to the capacity of the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the capacity of the workspace of the notebook.
    lakehouse : str | uuid.UUID, default=None
        The name or ID of the lakehouse.
        Defaults to None which resolves to the lakehouse attached to the notebook.
    table_properties: PropertyBag, default=None
        A collection of property values that the provision_perf_lab_lakehouse function passes to the table_generator function.
        The properties in the property bag are specific to the table_generator function.
    table_generator
        A callback function to generate and persist the actual Delta tables in the lakehouse.

    Returns
    -------
    Tuple[UUID, UUID]
        A tuple of the provisioned workspace and lakehouse IDs.
    """

    # Resolve the workspace name and id and provision a workspace if it doesn't exist.
    (workspace_name, workspace_id) = _get_or_create_workspace(
        workspace=workspace, capacity_id=capacity_id,
        description="A master workspace with a sample lakehouse and a Direct Lake semantic model that uses the Delta tables from the sample lakehouse.")

    # Resolve the lakehouse name and id and provision a lakehouse if it doesn't exist.
    (lakehouse_name, lakehouse_id) = _get_or_create_lakehouse(
        lakehouse=lakehouse, workspace=workspace_id,
        description="A lakehouse with a small number of automatically generated sample Delta tables.")

    # Call the provided callback function to generate the Delta tables.
    if table_generator is not None:
        table_generator(
            workspace_id=workspace_id,
            lakehouse_id=lakehouse_id,
            table_properties=table_properties)
        
    return(workspace_id, lakehouse_id)

def _get_sample_tables_property_bag(
    start_date: Optional[str | date] = None,
    years: Optional[int] = 4,
    fact_rows_in_millions: Optional[int] = 100,
    num_fact_tables: Optional[int] = 1,        
) -> PropertyBag:
    """
    Generates a property bag for the provision_sample_delta_tables function.

    Parameters
    ----------
    start_date : str | date, default=None
        The start date for the date table. If specified as a string, must adhere to the format "%Y-%m-%d", such as "2025-01-25".
        Defaults to None which resolves to the current date minus the specified years.
    years : int, default=4
        The number of years that the date table covers.
        The value must be greater than 0. Defaults to 4.
    fact_rows_in_millions : int, default=100
        The number of transactions in the sales table(s) in millions.
        The value must be greater than 0. Defaults to 100 for 100 million rows.
    num_fact_tables : int, default=1
        The number of fact table(s) to generate for the lakehouse.
        The value must be greater than 0. Defaults to 1.

    Returns
    -------
    PropertyBag
        A property bag wrapping the parameters passed into this function.

    """

    property_bag = PropertyBag()
    property_bag.add_property("start_date", start_date)
    property_bag.add_property("years", years)
    property_bag.add_property("fact_rows_in_millions", fact_rows_in_millions)
    property_bag.add_property("num_fact_tables", num_fact_tables)
   
    return property_bag

def provision_sample_delta_tables(
    workspace_id: UUID,
    lakehouse_id: UUID,
    table_properties: Optional[PropertyBag] = None,
):
    """
    Generates sample data for a date table.

    Parameters
    ----------
    workspace_id : uuid.UUID
        The Fabric workspace ID where the lakehouse is located.
    lakehouse_id : uuid.UUID
        The ID of the lakehouse where the delta tables should be added.
    table_properties: PropertyBag, default=None
        An arbirary collection of property values that the provision_perf_lab_lakehouse function passes to the table_generator function.
        The properties in the property bag are specific to the table_generator function.
    """

    start_date = table_properties.get_property('start_date')
    years = table_properties.get_property('years')
    fact_rows_in_millions = table_properties.get_property('fact_rows_in_millions')
    num_fact_tables = table_properties.get_property('num_fact_tables')        

        
    # Generate and persist the sample Delta tables in the lakehouse.
    _save_as_delta_table(
        _get_dates_df(start_date, years),
        "date",
        lakehouse_id,
        workspace_id,
        )

    _save_as_delta_table(
        _get_measure_table_df(),
        "measuregroup",
        lakehouse_id,
        workspace_id,
        )

    _save_as_delta_table(
        _get_product_categories_df(),
        "productcategory",
        lakehouse_id,
        workspace_id,
        )

    _save_as_delta_table(
        _get_geography_df(),
        "geography",
        lakehouse_id,
        workspace_id,
        )

    for i in range(1, num_fact_tables+1):
        _save_as_delta_table(
            _get_sales_df(start_date, years, fact_rows_in_millions, i),
            f"sales_{i}",
            lakehouse_id,
            workspace_id,
            )
        
def _generate_onelake_shared_expression(
    workspace_id: UUID,
    item_id: UUID,
) -> str:
    """
    Dynamically generates the M expression used by a Direct Lake model in OneLake mode for a given artifact.

    Parameters
    ----------
    workspace_id : UUID
        The ID of the Fabric workspace in which the Fabric item that owns the Delta tables is located.
    item_id : UUID
        The ID of the Fabric item that owns the Delta tables.

    Returns
    -------
    str
        Shows the expression which can be used to connect a Direct Lake semantic model to the DataLake.
    """

    # Get the dfs endpoint of the workspace
    client = fabric.FabricRestClient()
    response = client.get(f"/v1/workspaces/{workspace_id}")
    if response.status_code != 200:
        raise FabricHTTPException(response)

    olEPs = response.json().get("oneLakeEndpoints")
    dfsEP = olEPs.get("dfsEndpoint")
    
    start_expr = "let\n\tdatabase = "
    end_expr = "\nin\n\tdatabase"
    mid_expr = f"AzureStorage.DataLake(\"{dfsEP}/{workspace_id}/{item_id}\")"

    return f"{start_expr}{mid_expr}{end_expr}"

def provision_sample_semantic_model(
    workspace: str | UUID,
    lakehouse: str | UUID,
    semantic_model_name: str,
    semantic_model_mode: Optional[str] = "OneLake",
) -> Tuple[str, UUID]:
    """
    Creates a semantic model in Direct Lake mode in the specified workspace using the specified lakehouse as the data source.
    Assumes a specific structure of Delta tables provisioned using the perf_lab sample functions.

    Parameters
    ----------
    workspace : str | uuid.UUID
        The Fabric workspace name or ID where the semantic model should be created.
        The workspace must be specified and must exist or the function fails with a WorkspaceNotFoundException.
    lakehouse : str | uuid.UUID
        The name or ID of the lakehouse that the semantic model should use as the data source.
        The lakehouse must be specified and must exist in the specified workspace or the function fails with a ValueException stating that the lakehouse was not found.
    semantic_model_name : str
        The name of the semantic model. The semantic model name must be specified. 
        If a model with the same name already exists, the function fails with a ValueException due to a naming conflict. 
    semantic_model_mode : str, Default = OneLake. Options: 'SQL', 'OneLake'.
        An optional parameter to specify the mode of the semantic model. Two modes are supported: SQL and OneLake. By default, the function generates a Direct Lake model in OneLake mode.
    Returns
    -------
    Tuple[str, UUID]
        A tuple holding the name and ID of the created semantic model.
    """

    # Verify that the mode is valid.
    semantic_model_modes = ["SQL", "ONELAKE"]
    semantic_model_mode = semantic_model_mode.upper()
    if semantic_model_mode not in semantic_model_modes:
        raise ValueError(
            f"{icons.red_dot} Invalid semantic model mode. Valid options: {semantic_model_modes}."
        )

    # Make sure the workspace exists. Raises WorkspaceNotFoundException otherwise.
    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)

    # Raises a ValueError if there's no lakehouse with the specified name in the workspace.
    (lakehouse_name, lakehouse_id) = resolve_lakehouse_name_and_id(
        lakehouse=lakehouse, workspace=workspace_name)

    # Create the intial semantic model
    tables = lh.get_lakehouse_tables(lakehouse=lakehouse_id, workspace=workspace_id)
    table_names = tables['Table Name'].tolist()
    print(f"{icons.table_icon} {len(table_names)} tables found in lakehouse '{lakehouse_name}'.")

    # Create the intial semantic model
    print(f"{icons.in_progress} Creating Direct Lake semantic model '{semantic_model_name}' in workspace '{workspace_name}'.")
    directlake.generate_direct_lake_semantic_model(
        workspace=workspace_id, 
        lakehouse=lakehouse_name,
        dataset=semantic_model_name,
        lakehouse_tables=table_names,
        refresh=False)
    
    (dataset_name, dataset_id) = resolve_dataset_name_and_id(
        dataset=semantic_model_name, workspace=workspace_id)

    print(f"{icons.in_progress} Adding final touches to Direct Lake semantic model '{semantic_model_name}' in workspace '{workspace_name}'.")
    with connect_semantic_model(dataset=semantic_model_name, workspace=workspace_id, readonly=False) as tom:
        # if the semantic_model_mode is OneLake
        # convert the data access expression in the model to Direct Lake on 
        if semantic_model_mode == semantic_model_modes[1]:
            expression_name = "DatabaseQuery"
            expr = _generate_onelake_shared_expression(workspace_id, lakehouse_id)       

            if not any(e.Name == expression_name for e in tom.model.Expressions):
                tom.add_expression(name=expression_name, expression=expr)
            else:
                tom.model.Expressions[expression_name].Expression = expr
            
            # Also remove the schemaName property from all the partitions
            for t in tom.model.Tables:
                for p in t.Partitions:
                    p.Source.SchemaName = ""            
            
        tom.model.SaveChanges()
        print(f"{icons.green_dot} Direct Lake semantic model '{semantic_model_name}' converted to Direct Lake on OneLake mode.")

        # Clean up table names and source lineage tags
        for t in tom.model.Tables:
            for c in t.Columns:
                if c.Name.startswith("RowNumber") == False:
                    c.SourceLineageTag = c.Name
            
            if semantic_model_mode == semantic_model_modes[1]:
                t.SourceLineageTag = t.SourceLineageTag.replace("[dbo].", "")

            if t.SourceLineageTag == "[dbo].[sales_1]" or t.SourceLineageTag == "[sales_1]":
                t.set_Name("Sales")
            elif t.SourceLineageTag == "[dbo].[measuregroup]" or t.SourceLineageTag == "[measuregroup]": 
                t.set_Name("Pick a measure")
            elif t.SourceLineageTag == "[dbo].[productcategory]" or t.SourceLineageTag == "[productcategory]": 
                t.set_Name("Product")
            else:
                t.set_Name(t.Name.capitalize())            

        tom.model.SaveChanges()
        print(f"{icons.green_dot} Table names updated.")

        # Mark as date table and create relationships
        tom.mark_as_date_table(table_name="Date", column_name="Date")

        tom.add_relationship(
            from_table="Sales",
            from_column="DateID",
            to_table="Date",
            to_column="DateID",
            from_cardinality="Many",
            to_cardinality="One",
            cross_filtering_behavior="OneDirection",
            security_filtering_behavior="OneDirection",
            rely_on_referential_integrity=False,
            is_active=True
        )
        tom.add_relationship(
            from_table="Sales",
            from_column="GeoID",
            to_table="Geography",
            to_column="GeoID",
            from_cardinality="Many",
            to_cardinality="One",
            cross_filtering_behavior="OneDirection",
            security_filtering_behavior="OneDirection",
            rely_on_referential_integrity=False,
            is_active=True
        )
        tom.add_relationship(
            from_table="Sales",
            from_column="ProductCategoryID",
            to_table="Product",
            to_column="ProductCategoryID",
            from_cardinality="Many",
            to_cardinality="One",
            cross_filtering_behavior="OneDirection",
            security_filtering_behavior="OneDirection",
            rely_on_referential_integrity=False,
            is_active=True
        )

        tom.model.SaveChanges()
        print(f"{icons.green_dot} Table relationships added.")

        # Mark measures
        tom.add_measure(
            table_name = "Pick a measure",
            measure_name = "Orders",
            description = "Counts the total number of rows in the Sales table, representing the total number of orders.",
            expression = "COUNTROWS(Sales)",
            format_string = "#,0"
        )
        tom.add_measure(
            table_name = "Pick a measure",
            measure_name = "Sales",
            description = "Calculates the total sales by summing all the sales values.",
            expression = "SUM(Sales[sales])",
            format_string_expression = f"""
                SWITCH(
                    TRUE(),
                    SELECTEDMEASURE() < 1000,"$#,##0",
                    SELECTEDMEASURE() < 1000000, "$#,##0,.0K",
                    SELECTEDMEASURE() < 1000000000, "$#,##0,,.0M",
                    SELECTEDMEASURE() < 1000000000000,"$#,##0,,,.0B",
                    "$#,##0,,,,.0T"
                )
                """ 
        )
        tom.add_measure(
            table_name = "Pick a measure",
            measure_name = "Costs",
            description = "Calculates the total costs from the 'Sales' table.",
            expression = "SUM(Sales[costs])",
            format_string = "\$#,0;(\$#,0);\$#,0"
        )
        tom.add_measure(
            table_name = "Pick a measure",
            measure_name = "Profit",
            description = "Calculates the profit by subtracting costs from sales.",
            expression = "[sales] - [costs]",
            format_string = "\$#,0;(\$#,0);\$#,0"
        )
        tom.add_measure(
            table_name = "Pick a measure",
            measure_name = "Profit Margin",
            description = "Calculates the profit margin by dividing the profit by sales, returning a blank value if the sales are zero.",
            expression = "DIVIDE([profit],[sales],BLANK())",
            format_string = "#,0.00%;-#,0.00%;#,0.00%"
        )
        tom.add_measure(
            table_name = "Pick a measure",
            measure_name = "Avg Profit Per Order",
            description = "Calculates the average profit per order by dividing the total profit by the total number of orders.",
            expression = "DIVIDE([profit],[orders],0)",
            format_string = "\$#,0;(\$#,0);\$#,0",
            display_folder = "Avg"
        )
        tom.add_measure(
            table_name = "Pick a measure",
            measure_name = "Avg Sales Per Order",
            description = "Calculates the average sales per order by dividing the total sales by the total number of orders.",
            expression = "DIVIDE([sales],[orders],0)",
            format_string = "\$#,0;(\$#,0);\$#,0",
            display_folder = "Avg"
        )
        tom.add_measure(
            table_name = "Pick a measure",
            measure_name = "Avg Costs Per Order",
            description = "Calculates the average cost per order by dividing the total costs by the number of orders.",
            expression = "DIVIDE([Costs],[orders],0)",
            format_string = "\$#,0;(\$#,0);\$#,0",
            display_folder = "Avg"
        )

        tom.model.SaveChanges()
        print(f"{icons.green_dot} Measures created.")

        # Update columns
        tom.update_column(
            table_name="Pick a measure",
            column_name="Col1",
            hidden = True
        )
        tom.set_sort_by_column(
            table_name="Date",
            column_name="Month",
            sort_by_column="MonthOfYear"
        )
        tom.set_sort_by_column(
            table_name="Date",
            column_name="MonthYear",
            sort_by_column="Monthly"
        )
        tom.set_sort_by_column(
            table_name="Date",
            column_name="DayOfWeek",
            sort_by_column="DayOfWeekNum"
        )
        tom.update_column(
            table_name="Date",
            column_name="Date",
            format_string="dd mmm yyyy"
        )
        tom.update_column(
            table_name="Date",
            column_name="Monthly",
            format_string="mmm yyyy"
        )

        tom.model.SaveChanges()
        print(f"{icons.green_dot} Table columns updated.")


        # Add calc items and a hierarchy
        tom.add_calculated_table(
            name="xTables",
            expression="INFO.VIEW.TABLES()"
        )
        tom.add_field_parameter(
            table_name="Field parameter",
            objects=["[Orders]", "[Sales]", "[Costs]", "[Profit]"],
            object_names=["Orders","Sales","Costs","Profit"]
        )
        tom.add_calculation_group(
            name="Time intelligence",
            precedence=1
        )
        tom.model.Tables['Time intelligence'].Columns['Name'].set_Name("Time calculation")
        tom.add_calculation_item(
            table_name="Time intelligence",
            calculation_item_name="Current",
            expression="SELECTEDMEASURE()",
            ordinal=1
        )
        tom.add_calculation_item(
            table_name="Time intelligence",
            calculation_item_name="MTD",
            expression="CALCULATE(SELECTEDMEASURE(), DATESMTD('Date'[Date]))",
            ordinal=2
        )
        tom.add_calculation_item(
            table_name="Time intelligence",
            calculation_item_name="QTD",
            expression="CALCULATE(SELECTEDMEASURE(), DATESQTD('Date'[Date]))",
            ordinal=3
        )
        tom.add_calculation_item(
            table_name="Time intelligence",
            calculation_item_name="YTD",
            expression="CALCULATE(SELECTEDMEASURE(), DATESYTD('Date'[Date]))",
            ordinal=4
        )
        tom.add_calculation_item(
            table_name="Time intelligence",
            calculation_item_name="PY",
            expression="CALCULATE(SELECTEDMEASURE(), SAMEPERIODLASTYEAR('Date'[Date]))",
            ordinal=5
        )
        tom.add_calculation_item(
            table_name="Time intelligence",
            calculation_item_name="PY MTD",
            expression= f"""
            CALCULATE(
                SELECTEDMEASURE(),
                SAMEPERIODLASTYEAR('Date'[Date]),
                'Time Intelligence'[Time Calculation] = "MTD"
                )
            """,
            ordinal=6
        )
        tom.add_calculation_item(
            table_name="Time intelligence",
            calculation_item_name="PY QTD",
            expression= f"""
            CALCULATE(
                SELECTEDMEASURE(),
                SAMEPERIODLASTYEAR('Date'[Date]),
                'Time Intelligence'[Time Calculation] = "QTD"
                )
            """,
            ordinal=7
        )
        tom.add_calculation_item(
            table_name="Time intelligence",
            calculation_item_name="YOY",
            expression= f"""
            SELECTEDMEASURE() -
            CALCULATE(
                SELECTEDMEASURE(),
            'Time Intelligence'[Time Calculation] = "PY"
            )
            """,
            ordinal=8
        )
        tom.add_calculation_item(
            table_name="Time intelligence",
            calculation_item_name="YOY%",
            expression= f"""
            DIVIDE(
                CALCULATE(
                    SELECTEDMEASURE(),
                    'Time Intelligence'[Time Calculation]="YOY"
                ),
                CALCULATE(
                    SELECTEDMEASURE(),
                    'Time Intelligence'[Time Calculation]="PY"
                )
            )
            """,
            format_string_expression = f""" "#,##0.0%" """,
            ordinal=9
        )

        print(f"{icons.green_dot} Calculation items added.")

        tom.add_hierarchy(
            table_name="Date",
            hierarchy_name="Calendar",
            columns=["Year","Month","Date"]
        )
        print(f"{icons.green_dot} Calendar hierarchy created.")

        return (dataset_name, dataset_id)
    
def deprovision_perf_lab_lakehouses(
    test_definitions: DataFrame,
)->None:
    """
    Deprovisions lakehouses listed in the test definitions.
    These lakehouses may contain tables other then the perf lab tables, 
    which will all be removed together with the lakehouses.

    Parameters
    ----------
    test_definitions : DataFrame
        A spark dataframe with the semantic model and data source definitions, such as the a dataframe returned by the _get_test_definitions_df() function.

    Returns
    -------
    None
    """

    lakehouses_df = test_definitions \
        .filter(test_definitions["DatasourceType"] == "Lakehouse"). \
        dropDuplicates(['DatasourceName', 'DatasourceWorkspace'])
    
    if lakehouses_df.count() > 0:
        print(f"{icons.in_progress} Deleting '{lakehouses_df.count()}' lakehouses.")

        for l in lakehouses_df.dropDuplicates(['DatasourceName', 'DatasourceWorkspace']).collect():

            lakehouse_name = l['DatasourceName']
            (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace=l['DatasourceWorkspace'])

            notebookutils.lakehouse.delete(lakehouse_name, workspace_id)
            print(f"{icons.checked} Lakehouse '{lakehouse_name}' in workspace '{workspace_name}' deleted successfully.")
    else:
        print(f"{icons.red_dot} No lakehouses found in the test definitions.")

def deprovision_perf_lab_models(
    test_definitions: DataFrame,
    masters_and_clones: Optional[bool]=False
)->None:
    """
    Deprovisions test clones and optionally also the master semantic models listed in the test definitions.

    Parameters
    ----------
    test_definitions : DataFrame
        A spark dataframe with the semantic model and data source definitions, such as the a dataframe returned by the _get_test_definitions_df() function.
    masters_and_clones : bool, Default = False
        A flag indicating if master semantic models should be deleted together with their test model clones.

    Returns
    -------
    None
    """

    masters_df = test_definitions.dropDuplicates(['MasterDataset', 'MasterWorkspace'])
    if masters_and_clones and masters_df.count() > 0:
        print(f"{icons.in_progress} Deleting '{masters_df.count()}' master semantic models.")
        for m in masters_df.collect():
            try:
                (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace=m['MasterWorkspace'])
                (dataset_name, dataset_id) = resolve_dataset_name_and_id(dataset=m['MasterDataset'] , workspace=workspace_id)

                client = fabric.FabricRestClient()
                response = client.delete(
                    f"/v1/workspaces/{workspace_id}/semanticModels/{dataset_id}")

                if response.status_code != 200:
                    print(f"{icons.red_dot} response.")
                else:
                    print(f"{icons.checked} Master semantic model '{dataset_name}' in workspace '{workspace_name}' deleted successfully.")

            except Exception as e:
                print(f"{icons.red_dot} {e}")

    clones_df = test_definitions.dropDuplicates(['TargetDataset', 'TargetWorkspace'])
    if clones_df.count() > 0:
        print(f"{icons.in_progress} Deleting '{clones_df.count()}' test semantic models.")
        for c in clones_df.collect():
            try:
                (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace=c['TargetWorkspace'])
                (dataset_name, dataset_id) = resolve_dataset_name_and_id(dataset=c['TargetDataset'] , workspace=workspace_id)

                client = fabric.FabricRestClient()
                response = client.delete(
                    f"/v1/workspaces/{workspace_id}/semanticModels/{dataset_id}")

                if response.status_code != 200:
                    print(f"{icons.red_dot} response.")
                else:
                    print(f"{icons.checked} Test semantic model '{dataset_name}' in workspace '{workspace_name}' deleted successfully.")

            except Exception as e:
                print(f"{icons.red_dot} {e}")
    else:
        print(f"{icons.red_dot} No test semantic models found in the test definitions.")  