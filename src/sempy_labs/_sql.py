import sempy.fabric as fabric
import pandas as pd
from typing import Optional, Union, List
from sempy._utils._log import log
import struct
from itertools import chain, repeat
from sempy.fabric.exceptions import FabricHTTPException
from sempy_labs._helper_functions import (
    resolve_warehouse_id,
    resolve_lakehouse_id,
    resolve_workspace_name_and_id,
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
        name: str,
        workspace: Optional[Union[str, UUID]] = None,
        timeout: Optional[int] = None,
        endpoint_type: str = "warehouse",
    ):
        from sempy.fabric._token_provider import SynapseTokenProvider
        import pyodbc

        (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)

        # Resolve the appropriate ID (warehouse or lakehouse)
        if endpoint_type == "warehouse":
            resource_id = resolve_warehouse_id(warehouse=name, workspace=workspace_id)
        else:
            resource_id = resolve_lakehouse_id(lakehouse=name, workspace=workspace_id)

        # Get the TDS endpoint
        client = fabric.FabricRestClient()
        response = client.get(
            f"v1/workspaces/{workspace_id}/{endpoint_type}s/{resource_id}"
        )
        if response.status_code != 200:
            raise FabricHTTPException(response)

        if endpoint_type == "warehouse":
            tds_endpoint = response.json().get("properties", {}).get("connectionString")
        else:
            tds_endpoint = (
                response.json()
                .get("properties", {})
                .get("sqlEndpointProperties", {})
                .get("connectionString")
            )

        # Set up the connection string
        access_token = SynapseTokenProvider()()
        tokenstruct = _bytes2mswin_bstr(access_token.encode())
        conn_str = f"DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={tds_endpoint};DATABASE={name};Encrypt=Yes;"

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
        warehouse: str,
        workspace: Optional[Union[str, UUID]] = None,
        timeout: Optional[int] = None,
    ):
        super().__init__(
            name=warehouse,
            workspace=workspace,
            timeout=timeout,
            endpoint_type="warehouse",
        )


class ConnectLakehouse(ConnectBase):
    def __init__(
        self,
        lakehouse: str,
        workspace: Optional[Union[str, UUID]] = None,
        timeout: Optional[int] = None,
    ):
        super().__init__(
            name=lakehouse,
            workspace=workspace,
            timeout=timeout,
            endpoint_type="lakehouse",
        )
