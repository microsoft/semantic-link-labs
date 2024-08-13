import sempy.fabric as fabric
import pandas as pd
import re
from sempy_labs._helper_functions import retry
from pyspark.sql import SparkSession
from sempy_labs.tom import connect_semantic_model
from typing import Optional
from sempy._utils._log import log
import sempy_labs._icons as icons


@log
def refresh_calc_tables(dataset: str, workspace: Optional[str] = None):
    """
    Recreates the delta tables in the lakehouse based on the DAX expressions stored as model annotations in the Direct Lake semantic model.

    Parameters
    ----------
    dataset : str
        Name of the semantic model.
    workspace : str, default=None
        The Fabric workspace name.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    spark = SparkSession.builder.getOrCreate()
    workspace = fabric.resolve_workspace_name(workspace)

    @retry(
        sleep_time=1,
        timeout_error_message=f"{icons.red_dot} Function timed out after 1 minute",
    )
    def dyn_connect():
        with connect_semantic_model(
            dataset=dataset, readonly=True, workspace=workspace
        ) as tom:

            tom.model

    dyn_connect()

    with connect_semantic_model(
        dataset=dataset, readonly=True, workspace=workspace
    ) as tom:
        for a in tom.model.Annotations:
            if any(a.Name == t.Name for t in tom.model.Tables):
                tName = a.Name
                query = a.Value

                if not query.startswith("EVALUATE"):
                    daxquery = "EVALUATE \n" + query
                else:
                    daxquery = query

                try:
                    df = fabric.evaluate_dax(
                        dataset=dataset,
                        dax_string=daxquery,
                        workspace=workspace,
                    )

                    # Update column names for non-field parameters
                    if query.find("NAMEOF") == -1:
                        for old_column_name in df.columns:
                            pattern = r"\[([^\]]+)\]"

                            matches = re.findall(pattern, old_column_name)
                            new_column_name = matches[0]
                            new_column_name = new_column_name.replace(" ", "")

                            df.rename(
                                columns={old_column_name: new_column_name},
                                inplace=True,
                            )

                            # Update data types for lakehouse columns
                            dataType = next(
                                str(c.DataType)
                                for c in tom.all_columns()
                                if c.Parent.Name == tName
                                and c.SourceColumn == new_column_name
                            )
                            # dfC_type = dfC[(dfC['Table Name'] == tName) & (dfC['Source'] == new_column_name)]
                            # dataType = dfC_type['Data Type'].iloc[0]

                            if dataType == "Int64":
                                df[new_column_name] = df[new_column_name].astype(int)
                            elif dataType in ["Decimal", "Double"]:
                                df[new_column_name] = df[new_column_name].astype(float)
                            elif dataType == "Boolean":
                                df[new_column_name] = df[new_column_name].astype(bool)
                            elif dataType == "DateTime":
                                df[new_column_name] = pd.to_datetime(
                                    df[new_column_name]
                                )
                            else:
                                df[new_column_name] = df[new_column_name].astype(str)
                    # else:
                    #    second_column_name = df.columns[1]
                    #    third_column_name = df.columns[2]
                    #    df[third_column_name] = df[third_column_name].astype(int)

                    # Remove calc columns from field parameters
                    #    mask = df[second_column_name].isin(dfC_filt['Full Column Name'])
                    #    df = df[~mask]

                    delta_table_name = tName.replace(" ", "_")
                    print(
                        f"{icons.in_progress} Refresh of the '{delta_table_name}' table within the lakehouse is in progress..."
                    )

                    spark_df = spark.createDataFrame(df)
                    spark_df.write.mode("overwrite").format("delta").saveAsTable(
                        delta_table_name
                    )
                    print(
                        f"{icons.green_dot} Calculated table '{tName}' has been refreshed as the '{delta_table_name.lower()}' table in the lakehouse."
                    )
                except Exception as e:
                    raise ValueError(
                        f"{icons.red_dot} Failed to create calculated table '{tName}' as a delta table in the lakehouse."
                    ) from e
