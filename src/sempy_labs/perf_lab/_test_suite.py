from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType
from uuid import UUID
from typing import Optional
import sempy_labs._icons as icons

from sempy_labs._helper_functions import (
    _save_as_delta_table,
    _read_delta_table
)

class TestDefinition:
    """
        A test definition must have at least the following fields, but can also have additional arbitrary fields.
        +----------+----------+----------------+-------------+---------------+-------------+---------------+-------------------+--------------+
        | QueryId|   QueryText| MasterWorkspace|MasterDataset|TargetWorkspace|TargetDataset| DatasourceName|DatasourceWorkspace|DatasourceType|
        +----------+----------+----------------+-------------+---------------+-------------+---------------+-------------------+--------------+
    """    
    def __init__(self, **kwargs):
        self.fields = ['QueryId', 'QueryText', 'MasterWorkspace', 'MasterDataset', 
                       'TargetWorkspace', 'TargetDataset', 'DatasourceName', 
                       'DatasourceWorkspace', 'DatasourceType']
        for field in self.fields:
            setattr(self, field, kwargs.get(field, None))
        # Set any additional fields
        for key, value in kwargs.items():
            if key not in self.fields:
                setattr(self, key, value)

    def add(self, key, value):
        setattr(self, key, value)
        
    def get_keys(self):
        return [key for key in self.__dict__.keys() if key != 'fields']

    def get_values(self):
        return tuple(value for key, value in self.__dict__.items() if key != 'fields')

    def to_spark_schema(self):
        schema_fields = [StructField(field, StringType(), True) for field in self.get_keys()]
        return StructType(schema_fields)

class TestSuite:
    """
        A test suite consists of an array of test definitions 
        and provides helpful methods to load and persist test definitions.
    """  
    def __init__(self, test_definitions=None):
        if test_definitions is None:
            test_definitions = []
        self.test_definitions = test_definitions

    def add_test_definition(self, test_definition):
        self.test_definitions.append(test_definition)

    def add_field(self, key, value):
        for test_def in self.test_definitions:
            test_def.add(key, value)

    def get_spark_schema(self):
        if self.test_definitions:
            return self.test_definitions[0].to_spark_schema()
        return None

    def to_df(self):
        spark = SparkSession.builder.getOrCreate()
        rows = [Row(*test_def.get_values()) for test_def in self.test_definitions]
        return spark.createDataFrame(rows, self.get_spark_schema())

    def load(self, 
            delta_table: str,
            lakehouse: Optional [str | UUID] = None,
            workspace: Optional [str | UUID] = None,
        ):
        """
        Loads test definitions from a Delta table in a Fabric lakehouse.
        The Delta table must have at least the following columns, but can also have additional arbitrary columns.
        +----------+----------+----------------+-------------+---------------+-------------+---------------+-------------------+--------------+
        | QueryId|   QueryText| MasterWorkspace|MasterDataset|TargetWorkspace|TargetDataset| DatasourceName|DatasourceWorkspace|DatasourceType|
        +----------+----------+----------------+-------------+---------------+-------------+---------------+-------------------+--------------+

        Parameters
        ----------
        delta_table : str
            The name or path of the delta table.
        lakehouse : uuid.UUID
            The Fabric lakehouse ID.
            Defaults to None which resolves to the lakehouse attached to the notebook.
        workspace : uuid.UUID
            The Fabric workspace ID where the specified lakehouse is located.
            Defaults to None which resolves to the workspace of the attached lakehouse
            or if no lakehouse attached, resolves to the workspace of the notebook.
        """
        spark = SparkSession.builder.getOrCreate()
        df = _read_delta_table(
            path = delta_table,
            lakehouse = lakehouse,
            workspace = workspace,
        )

        for row in df.collect():
            test_definition = TestDefinition(**row.asDict())
            self.add_test_definition(test_definition)

    def save_as(self,     
            delta_table_name: str,
            lakehouse: Optional [str | UUID] = None,
            workspace: Optional [str | UUID] = None,
        ):
        """
        Saves a spark dataframe as a delta table in a Fabric lakehouse.

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
        """

        _save_as_delta_table(
            dataframe = self.to_df(),
            delta_table_name = delta_table_name,
            lakehouse = lakehouse,
            workspace = workspace,
        )
    
    def merge(self, other):
        if not isinstance(other, TestSuite):
            raise ValueError("Can only merge with another TestSuite instance")
        self.test_definitions.extend(other.test_definitions)