import sempy.fabric as fabric
import pandas as pd
from typing import Optional, Union
from sempy._utils._log import log
import struct
import uuid
from itertools import chain, repeat
from sempy.fabric.exceptions import FabricHTTPException
import sempy_labs._icons as icons


def bytes2mswin_bstr(value: bytes) -> bytes:
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
    # The original code from the provided github URL applies 'Little-endian'
    # to the byte conversion in a python for-loop.
    # The following is code is assumed to be faster.
    encoded_bytes = bytes(chain.from_iterable(zip(value, repeat(0))))
    # The original code applies native endianness for encoding the length
    # prefix. My working assumption that also the length should be encoded
    # 'Little-endian', regardless of the CPU (and OS) this code is running on.
    return struct.pack("<i", len(encoded_bytes)) + encoded_bytes


class ConnectWarehouse:
    def __init__(
        self,
        warehouse: str,
        workspace: Optional[Union[str, uuid.UUID]] = None,
        timeout: Optional[int] = None,
    ):
        from sempy.fabric._token_provider import SynapseTokenProvider
        import pyodbc

        # get the TDS endpoint
        client = fabric.FabricRestClient()
        workspace = fabric.resolve_workspace_name(workspace)
        workspace_id = fabric.resolve_workspace_id(workspace)
        dfI = fabric.list_items(workspace=workspace, type="Warehouse")

        dfI_filt = dfI[dfI["Display Name"] == warehouse]
        if len(dfI_filt) == 0:
            raise ValueError(
                f"{icons.red_dot} Invalid warehouse. The '{warehouse}' warehouse does not exist within the '{workspace}'  workspace."
            )
        warehouse_id = dfI_filt["Id"].iloc[0]

        response = client.get(f"v1/workspaces/{workspace_id}/warehouses/{warehouse_id}")
        if response.status_code != 200:
            raise FabricHTTPException(response)

        tds_endpoint = response.json().get("properties", {}).get("connectionString")

        access_token = SynapseTokenProvider()()
        tokenstruct = bytes2mswin_bstr(access_token.encode())

        conn_str = f"DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={tds_endpoint};DATABASE={warehouse};Encrypt=Yes;"

        if timeout is not None:
            conn_str += f"Connect Timeout={timeout};"

        self.connection = pyodbc.connect(conn_str, attrs_before={1256: tokenstruct})

    @log
    def query(self, sql: str) -> pd.DataFrame:
        """
        Runs a SQL query against a Fabric Warehouse.

        Parameters
        ----------
        sql : str
            The SQL query.

        Returns
        -------
        pandas.DataFrame
            A pandas dataframe with the result of the SQL query.
        """
        cursor = None

        try:
            cursor = self.connection.cursor()
            cursor.execute(sql)

            return pd.DataFrame.from_records(
                cursor.fetchall(), columns=[col[0] for col in cursor.description]
            )
        finally:
            if cursor:
                cursor.close()

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.close()

    def close(self):
        self.connection.close()
