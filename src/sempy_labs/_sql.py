import pandas as pd
from typing import Optional, Union, List
from sempy._utils._log import log
import struct
from itertools import chain, repeat
from sempy_labs._helper_functions import (
    resolve_lakehouse_name_and_id,
    resolve_item_name_and_id,
    resolve_workspace_name_and_id,
    _base_api,
)
from uuid import UUID


def _bytes2mswin_bstr(value: bytes) -> bytes:
    """Convert a sequence of bytes into a (MS-Windows) BSTR (as bytes).

    See https://github.com/mkleehammer/pyodbc/issues/228#issuecomment-319190980
    for the original code.  It appears the input is converted to an
    MS-Windows BSTR (in 'Little-endian' format).

    See https://docs.microsoft.com/en-us/openspecs/windows_protocols/ms-dtyp\
       /692a42a9-06ce-4394-b9bc-5d2a50440168
    for more info on BSTR.

    :param value: the sequence of bytes to convert
    :return: the converted value (as a sequence of bytes)
    """

    encoded_bytes = bytes(chain.from_iterable(zip(value, repeat(0))))
    return struct.pack("<i", len(encoded_bytes)) + encoded_bytes


class ConnectBase:
    def __init__(
        self,
        item: str | UUID,
        workspace: Optional[Union[str, UUID]] = None,
        timeout: Optional[int] = None,
        endpoint_type: str = "warehouse",
    ):
        from sempy.fabric._credentials import get_access_token
        import pyodbc

        (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)

        # Resolve the appropriate ID and name (warehouse or lakehouse)
        if endpoint_type == "sqldatabase":
            # SQLDatabase is has special case for resolving the name and id
            (resource_name, resource_id) = resolve_item_name_and_id(
                item=item, type="SQLDatabase", workspace=workspace_id
            )
        elif endpoint_type == "lakehouse":
            (resource_name, resource_id) = resolve_lakehouse_name_and_id(
                lakehouse=item,
                workspace=workspace_id,
            )
        else:
            (resource_name, resource_id) = resolve_item_name_and_id(
                item=item, workspace=workspace_id, type=endpoint_type.capitalize()
            )

        endpoint_for_url = (
            "sqlDatabases" if endpoint_type == "sqldatabase" else f"{endpoint_type}s"
        )

        # Get the TDS endpoint
        response = _base_api(
            request=f"v1/workspaces/{workspace_id}/{endpoint_for_url}/{resource_id}"
        )

        if endpoint_type == "warehouse":
            tds_endpoint = response.json().get("properties", {}).get("connectionString")
        elif endpoint_type == "sqldatabase":
            tds_endpoint = response.json().get("properties", {}).get("serverFqdn")
        else:
            tds_endpoint = (
                response.json()
                .get("properties", {})
                .get("sqlEndpointProperties", {})
                .get("connectionString")
            )

        # Set up the connection string
        access_token = get_access_token("sql").token
        tokenstruct = _bytes2mswin_bstr(access_token.encode())
        if endpoint_type == "sqldatabase":
            conn_str = f"DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={tds_endpoint};DATABASE={resource_name}-{resource_id};Encrypt=Yes;"
        else:
            conn_str = f"DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={tds_endpoint};DATABASE={resource_name};Encrypt=Yes;"

        if timeout is not None:
            conn_str += f"Connect Timeout={timeout};"

        self.connection = pyodbc.connect(conn_str, attrs_before={1256: tokenstruct})

    @log
    def query(
        self, sql: Union[str, List[str]]
    ) -> Union[List[pd.DataFrame], pd.DataFrame, None]:
        """
        Runs a SQL or T-SQL query (or multiple queries) against a Fabric Warehouse/Lakehouse.

        Parameters
        ----------
        sql : str or List[str]
            A single SQL or T-SQL query, or a list of queries to be executed.

        Returns
        -------
        Union[List[pandas.DataFrame], pandas.DataFrame, None]
            A list of pandas DataFrames if multiple SQL queries return results,
            a single DataFrame if one query is executed and returns results, or None.
        """

        cursor = None
        results = []

        if isinstance(sql, str):
            sql = [sql]

        try:
            cursor = self.connection.cursor()

            for sql_query in sql:
                cursor.execute(sql_query)

                # Commit for non-select queries (like CREATE, INSERT, etc.)
                if not cursor.description:
                    self.connection.commit()
                else:
                    # Fetch and append results for queries that return a result set
                    result = pd.DataFrame.from_records(
                        cursor.fetchall(),
                        columns=[col[0] for col in cursor.description],
                    )
                    results.append(result)

            # Return results if any queries returned a result set
            return results if len(results) > 1 else (results[0] if results else None)

        finally:
            if cursor:
                cursor.close()

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.close()

    def close(self):
        self.connection.close()


class ConnectWarehouse(ConnectBase):
    def __init__(
        self,
        warehouse: str | UUID,
        workspace: Optional[Union[str, UUID]] = None,
        timeout: int = 30,
    ):
        """
        Run a SQL or T-SQL query against a Fabric Warehouse.

        Parameters
        ----------
        warehouse : str | uuid.UUID
            The name or ID of the Fabric warehouse.
        workspace : str | uuid.UUID, default=None
            The name or ID of the workspace.
            Defaults to None which resolves to the workspace of the attached lakehouse
            or if no lakehouse attached, resolves to the workspace of the notebook.
        timeout : int, default=30
            The timeout for the connection in seconds.
        """
        super().__init__(
            item=warehouse,
            workspace=workspace,
            timeout=timeout,
            endpoint_type="warehouse",
        )


class ConnectLakehouse(ConnectBase):
    def __init__(
        self,
        lakehouse: Optional[str | UUID] = None,
        workspace: Optional[Union[str, UUID]] = None,
        timeout: int = 30,
    ):
        """
        Run a SQL or T-SQL query against a Fabric lakehouse.

        Parameters
        ----------
        lakehouse : str | uuid.UUID, default=None
            The name or ID of the Fabric lakehouse.
            Defaults to None which resolves to the lakehouse attached to the notebook.
        workspace : str | uuid.UUID, default=None
            The name or ID of the workspace.
            Defaults to None which resolves to the workspace of the attached lakehouse
            or if no lakehouse attached, resolves to the workspace of the notebook.
        timeout : int, default=30
            The timeout for the connection in seconds.
        """
        super().__init__(
            item=lakehouse,
            workspace=workspace,
            timeout=timeout,
            endpoint_type="lakehouse",
        )


class ConnectSQLDatabase(ConnectBase):
    def __init__(
        self,
        sql_database: str | UUID,
        workspace: Optional[Union[str, UUID]] = None,
        timeout: int = 30,
    ):
        """
        Run a SQL or T-SQL query against a Fabric SQL database.

        Parameters
        ----------
        sql_database : str | uuid.UUID
            The name or ID of the Fabric SQL database.
        workspace : str | uuid.UUID, default=None
            The name or ID of the workspace.
            Defaults to None which resolves to the workspace of the attached lakehouse
            or if no lakehouse attached, resolves to the workspace of the notebook.
        timeout : int, default=30
            The timeout for the connection in seconds.
        """
        super().__init__(
            item=sql_database,
            workspace=workspace,
            timeout=timeout,
            endpoint_type="sqldatabase",
        )
