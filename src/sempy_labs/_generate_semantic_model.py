import sempy.fabric as fabric
import pandas as pd
import json
import os
from typing import Optional, List
from sempy._utils._log import log
from sempy_labs._helper_functions import (
    resolve_lakehouse_name,
    resolve_workspace_name_and_id,
    resolve_dataset_name_and_id,
    _conv_b64,
    _decode_b64,
    _base_api,
)
from sempy_labs.lakehouse._lakehouse import lakehouse_attached
import sempy_labs._icons as icons
from sempy_labs._refresh_semantic_model import refresh_semantic_model
from uuid import UUID


@log
def create_blank_semantic_model(
    dataset: str,
    compatibility_level: int = 1605,
    workspace: Optional[str | UUID] = None,
    overwrite: bool = True,
):
    """
    Creates a new blank semantic model (no tables/columns etc.).

    Parameters
    ----------
    dataset : str
        Name of the semantic model.
    compatibility_level : int, default=1605
        The compatibility level of the semantic model.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    overwrite : bool, default=False
        If set to True, overwrites the existing semantic model in the workspace if it exists.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    dfD = fabric.list_datasets(workspace=workspace_id, mode="rest")
    dfD_filt = dfD[dfD["Dataset Name"] == dataset]

    if len(dfD_filt) > 0 and not overwrite:
        raise ValueError(
            f"{icons.warning} The '{dataset}' semantic model already exists within the '{workspace_name}' workspace. The 'overwrite' parameter is set to False so the blank new semantic model was not created."
        )

    min_compat = 1500
    if compatibility_level < min_compat:
        raise ValueError(
            f"{icons.red_dot} Compatiblity level must be at least {min_compat}."
        )

    # If the model does not exist
    if len(dfD_filt) == 0:
        tmsl = f"""
        {{
            "createOrReplace": {{
            "object": {{
                "database": '{dataset}'
            }},
            "database": {{
                "name": '{dataset}',
                "compatibilityLevel": {compatibility_level},
                "model": {{
                    "cultures": [
                        {{
                            "name": "en-US",
                            "linguisticMetadata": {{
                                "content": {{
                                    "Version": "1.0.0",
                                    "Language": "en-US"
                                }},
                                "contentType": "json"
                            }}
                        }}
                    ],
                    "collation": "Latin1_General_100_BIN2_UTF8",
                    "dataAccessOptions": {{
                        "legacyRedirects": true,
                        "returnErrorValuesAsNull": true,
                    }},
                    "defaultPowerBIDataSourceVersion": "powerBI_V3",
                    "sourceQueryCulture": "en-US",
                    }}
                }}
            }}
        }}
        """
    else:
        tmsl = f"""
        {{
            "createOrReplace": {{
            "object": {{
                "database": '{dataset}'
            }},
            "database": {{
                "name": '{dataset}',
                "compatibilityLevel": {compatibility_level},
                "model": {{
                "culture": "en-US",
                "defaultPowerBIDataSourceVersion": "powerBI_V3"
                }}
            }}
            }}
        }}
        """

    fabric.execute_tmsl(script=tmsl, workspace=workspace_id)

    return print(
        f"{icons.green_dot} The '{dataset}' semantic model was created within the '{workspace_name}' workspace."
    )


@log
def create_semantic_model_from_bim(
    dataset: str, bim_file: dict, workspace: Optional[str | UUID] = None
):
    """
    Creates a new semantic model based on a Model.bim file.

    This is a wrapper function for the following API: `Items - Create Semantic Model <https://learn.microsoft.com/rest/api/fabric/semanticmodel/items/create-semantic-model>`_.

    Parameters
    ----------
    dataset : str
        Name of the semantic model.
    bim_file : dict
        The model.bim file.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)

    dfI = fabric.list_datasets(workspace=workspace_id, mode="rest")
    dfI_filt = dfI[(dfI["Dataset Name"] == dataset)]

    if not dfI_filt.empty:
        raise ValueError(
            f"{icons.red_dot} The '{dataset}' semantic model already exists as a semantic model in the '{workspace_name}' workspace."
        )

    defPBIDataset = {"version": "1.0", "settings": {}}
    payloadPBIDefinition = _conv_b64(defPBIDataset)
    payloadBim = _conv_b64(bim_file)

    payload = {
        "displayName": dataset,
        "definition": {
            "parts": [
                {
                    "path": "model.bim",
                    "payload": payloadBim,
                    "payloadType": "InlineBase64",
                },
                {
                    "path": "definition.pbidataset",
                    "payload": payloadPBIDefinition,
                    "payloadType": "InlineBase64",
                },
            ]
        },
    }

    _base_api(
        request=f"v1/workspaces/{workspace_id}/semanticModels",
        payload=payload,
        method="post",
        lro_return_status_code=True,
        status_codes=[201, 202],
    )

    print(
        f"{icons.green_dot} The '{dataset}' semantic model has been created within the '{workspace_name}' workspace."
    )


@log
def update_semantic_model_from_bim(
    dataset: str | UUID, bim_file: dict, workspace: Optional[str | UUID] = None
):
    """
    Updates a semantic model definition based on a Model.bim file.

    This is a wrapper function for the following API: `Items - Update Semantic Model Definition <https://learn.microsoft.com/rest/api/fabric/semanticmodel/items/update-semantic-model-definition>`_.

    Parameters
    ----------
    dataset : str | uuid.UUID
        Name or ID of the semantic model.
    bim_file : dict
        The model.bim file.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    (dataset_name, dataset_id) = resolve_dataset_name_and_id(dataset, workspace_id)

    defPBIDataset = {"version": "1.0", "settings": {}}
    payloadPBIDefinition = _conv_b64(defPBIDataset)
    payloadBim = _conv_b64(bim_file)

    payload = {
        "displayName": dataset_name,
        "definition": {
            "parts": [
                {
                    "path": "model.bim",
                    "payload": payloadBim,
                    "payloadType": "InlineBase64",
                },
                {
                    "path": "definition.pbidataset",
                    "payload": payloadPBIDefinition,
                    "payloadType": "InlineBase64",
                },
            ]
        },
    }

    _base_api(
        request=f"v1/workspaces/{workspace_id}/semanticModels/{dataset_id}/updateDefinition",
        payload=payload,
        method="post",
        lro_return_status_code=True,
        status_codes=None,
    )

    print(
        f"{icons.green_dot} The '{dataset_name}' semantic model has been updated within the '{workspace_name}' workspace."
    )


@log
def deploy_semantic_model(
    source_dataset: str,
    source_workspace: Optional[str | UUID] = None,
    target_dataset: Optional[str] = None,
    target_workspace: Optional[str | UUID] = None,
    refresh_target_dataset: bool = True,
    overwrite: bool = False,
    perspective: Optional[str] = None,
    filters: Optional[dict] = None,
):
    """
    Deploys a semantic model based on an existing semantic model.

    Parameters
    ----------
    source_dataset : str
        Name of the semantic model to deploy.
    source_workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    target_dataset: str
        Name of the new semantic model to be created.
    target_workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID in which the new semantic model will be deployed.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    refresh_target_dataset : bool, default=True
        If set to True, this will initiate a full refresh of the target semantic model in the target workspace.
    overwrite : bool, default=False
        If set to True, overwrites the existing semantic model in the workspace if it exists.
    perspective : str, default=None
        Set this to the name of a perspective in the model and it will reduce the deployed model down to the tables/columns/measures/hierarchies within that perspective.
    filters : dict, default=None
        A dictionary of filters to apply to the tables in the model. This is only supported for Direct Lake models and for tables in Direct Lake mode.

        Supported operators: [=, <>, !=, <, <=, >, >=, IN, NOT IN]

        Format:
        {
            "TableName": ["ColumnName", "Operator", "Value"],
        }
        Example:
        filters = {
            "Geography": ["Country", "=", "Canada"],
            "Product": ["ModelName", "IN", ["Bikes", "Cars"]]
        }
    """

    (source_workspace_name, source_workspace_id) = resolve_workspace_name_and_id(
        source_workspace
    )

    if target_workspace is None:
        target_workspace_name = source_workspace_name
        target_workspace_id = fabric.resolve_workspace_id(target_workspace_name)
    else:
        (target_workspace_name, target_workspace_id) = resolve_workspace_name_and_id(
            target_workspace
        )

    if target_dataset is None:
        target_dataset = source_dataset

    if (
        target_dataset == source_dataset
        and target_workspace_name == source_workspace_name
    ):
        raise ValueError(
            f"{icons.red_dot} The 'dataset' and 'new_dataset' parameters have the same value. And, the 'workspace' and 'new_dataset_workspace' "
            f"parameters have the same value. At least one of these must be different. Please update the parameters."
        )

    dfD = fabric.list_datasets(workspace=target_workspace_id, mode="rest")
    dfD_filt = dfD[dfD["Dataset Name"] == target_dataset]
    if len(dfD_filt) > 0 and not overwrite:
        raise ValueError(
            f"{icons.warning} The '{target_dataset}' semantic model already exists within the '{target_workspace_name}' workspace. The 'overwrite' parameter is set to False so the source semantic model was not deployed to the target destination."
        )

    if filters is not None and perspective is None:
        raise ValueError(
            f"{icons.red_dot} The 'filters' parameter is only supported when the 'perspective' parameter is set to None. Please update the parameters."
        )

    if filters is not None:
        import sempy

        sempy.fabric._client._utils._init_analysis_services()
        import Microsoft.AnalysisServices.Tabular as TOM
        from sempy_labs._helper_functions import (
            find_transitive_incoming_relationships,
            create_abfss_path,
            _read_delta_table,
            save_as_delta_table,
        )
        from sempy_labs.tom import connect_semantic_model
        from sempy_labs.directlake._dl_helper import get_direct_lake_source
        from functools import reduce
        from pyspark.sql.functions import col
        from sempy_labs._sql import ConnectLakehouse

        valid_operators = ["=", "!=", "<>", "<", ">", "<=", ">=", "IN", "NOT IN"]
        write_mode = "overwrite"

        expanded_data = []
        for table_name, filter_details in filters.items():
            expanded_data.append([table_name] + filter_details)

        columns = ["TableName", "FilterColumn", "FilterOperator", "FilterValue"]
        filters_df = pd.DataFrame(expanded_data, columns=columns)

        with connect_semantic_model(
            dataset=source_dataset, workspace=source_workspace
        ) as tom:
            if not tom.is_direct_lake():
                raise ValueError(
                    f"{icons.red_dot} This is only supported for semantic models in Direct Lake mode."
                )

            (artifact_type, artifact_name, lakehouse_id, lakehouse_workspace_id) = (
                get_direct_lake_source(source_dataset, source_workspace)
            )

            if not artifact_type == "Lakehouse":
                raise ValueError(
                    f"{icons.red_dot} This is only supported for semantic models in Direct Lake mode. Cannot find lakehouse."
                )

            fabric.refresh_tom_cache(workspace=source_workspace)
            dfR = fabric.list_relationships(
                dataset=source_dataset, workspace=source_workspace
            )

            filter_conditions = []
            suffix = perspective.replace(" ", "")
            # Check object validity
            for _, r in filters_df.iterrows():
                table_name = r["TableName"]
                column_name = r["FilterColumn"]
                operator, value = r["FilterOperator"].upper(), r["FilterValue"]
                if not any(t.Name == table_name for t in tom.model.Tables):
                    raise ValueError(f"The '{table_name}' table does not exist.")
                if not any(
                    c.Parent.Name == table_name and c.Name == column_name
                    for c in tom.all_columns()
                ):
                    raise ValueError(
                        f"The '{table_name}'[{column_name}] column does not exist."
                    )
                if operator not in valid_operators:
                    raise ValueError(f"'{operator}' is not a valid operator.")
                partition_name = next(
                    p.Name for p in tom.model.Tables[table_name].Partitions
                )
                if (
                    tom.model.Tables[table_name].Partitions[partition_name].Mode
                    != TOM.ModeType.DirectLake
                ):
                    raise ValueError(
                        f"{icons.red_dot} Filtering is only valid for tables in Direct Lake mode."
                    )

            for _, r in filters_df.iterrows():
                table_name = r["TableName"]
                column_name = r["FilterColumn"]
                operator, value = r["FilterOperator"].upper(), r["FilterValue"]
                partition_name = next(
                    p.Name for p in tom.model.Tables[table_name].Partitions
                )
                entity_name = (
                    tom.model.Tables[table_name]
                    .Partitions[partition_name]
                    .Source.EntityName
                )
                source_column_name = (
                    tom.model.Tables[table_name].Columns[column_name].SourceColumn
                )

                path = create_abfss_path(
                    lakehouse_id, lakehouse_workspace_id, entity_name
                )
                new_lake_table_name = f"{entity_name}_{suffix}"
                dfL = _read_delta_table(path)

                # Filter dataframe
                if operator == "=":
                    filter_conditions.append(col(source_column_name) == value)
                elif operator in {"!=", "<>"}:
                    filter_conditions.append(col(source_column_name) != value)
                elif operator == "IN":
                    if isinstance(value, str):
                        value = [value]
                    filter_conditions.append(col(source_column_name).isin(value))
                elif operator == "NOT IN":
                    if isinstance(value, str):
                        value = [value]
                    filter_conditions.append(~col(source_column_name).isin(value))
                elif operator == ">":
                    filter_conditions.append(col(source_column_name) > value)
                elif operator == "<":
                    filter_conditions.append(col(source_column_name) < value)
                else:
                    raise NotImplementedError

                dfL_filt = dfL.filter(reduce(lambda x, y: x & y, filter_conditions))
                # Create delta table for filtered data
                save_as_delta_table(
                    dataframe=dfL_filt,
                    delta_table_name=new_lake_table_name,
                    write_mode=write_mode,
                    lakehouse=lakehouse_id,
                    workspace=lakehouse_workspace_id,
                )

                transitive_relations = find_transitive_incoming_relationships(
                    dfR, table_name
                ).sort_values(by="Degree")
                print(transitive_relations)

                alias_counter = 0
                alias_map = {entity_name: alias_counter}
                alias_counter += 1

                for _, r in transitive_relations.iterrows():
                    from_column = r["From Column"]
                    to_column = r["To Column"]
                    from_table = r["From Table"]
                    to_table = r["To Table"]
                    degree = r["Degree"]

                    from_partition = next(
                        p.Name for p in tom.model.Tables[from_table].Partitions
                    )
                    from_entity = (
                        tom.model.Tables[from_table]
                        .Partitions[from_partition]
                        .Source.EntityName
                    )
                    from_source_column = (
                        tom.model.Tables[from_table].Columns[from_column].SourceColumn
                    )

                    to_partition = next(
                        p.Name for p in tom.model.Tables[to_table].Partitions
                    )
                    to_entity = (
                        tom.model.Tables[to_table]
                        .Partitions[to_partition]
                        .Source.EntityName
                    )
                    to_source_column = (
                        tom.model.Tables[to_table].Columns[to_column].SourceColumn
                    )

                    new_lake_table_name = f"{from_entity}_{suffix}"

                    if from_entity not in alias_map:
                        alias_map[from_entity] = alias_counter
                        alias_counter += 1
                    if to_entity not in alias_map:
                        alias_map[to_entity] = alias_counter
                        alias_counter += 1

                    # Generate SQL query dynamically for multi-degree joins
                    join_conditions = []
                    last_entity = from_entity
                    last_column = from_source_column

                    filtered_table = (
                        table_name  # This is the final table where we apply the filter
                    )

                    # Build JOINs for each degree
                    for deg in range(degree, 0, -1):
                        rel = transitive_relations[
                            transitive_relations["Degree"] == deg
                        ].iloc[0]

                        join_from_table = rel["From Table"]
                        join_to_table = rel["To Table"]
                        join_from_column = rel["From Column"]
                        join_to_column = rel["To Column"]

                        join_from_entity = (
                            tom.model.Tables[join_from_table]
                            .Partitions[
                                next(
                                    p.Name
                                    for p in tom.model.Tables[
                                        join_from_table
                                    ].Partitions
                                )
                            ]
                            .Source.EntityName
                        )
                        join_to_entity = (
                            tom.model.Tables[join_to_table]
                            .Partitions[
                                next(
                                    p.Name
                                    for p in tom.model.Tables[join_to_table].Partitions
                                )
                            ]
                            .Source.EntityName
                        )

                        join_from_source_column = (
                            tom.model.Tables[join_from_table]
                            .Columns[join_from_column]
                            .SourceColumn
                        )
                        join_to_source_column = (
                            tom.model.Tables[join_to_table]
                            .Columns[join_to_column]
                            .SourceColumn
                        )

                        from_alias = f"{alias_map[join_from_entity]}"
                        to_alias = f"{alias_map[join_to_entity]}"

                        join_conditions.append(
                            f"LEFT JOIN {join_to_entity} AS T{to_alias} ON T{from_alias}.{join_from_source_column} = T{to_alias}.{join_to_source_column}\n"
                        )

                        last_entity = join_to_entity
                        last_column = join_to_source_column

                max_value = max(alias_map.values())
                # Final query with multiple JOINs
                query = f"""SELECT T0.*\nFROM {from_entity} AS T{max_value}\n{''.join(join_conditions)}WHERE T{alias_map.get(entity_name)}.{source_column_name} {operator} '{value}'
                """
                # print(query)
                with ConnectLakehouse(
                    lakehouse=lakehouse_id, workspace=lakehouse_workspace_id
                ) as sql:
                    df_result = sql.query(query)

                save_as_delta_table(
                    dataframe=df_result,
                    delta_table_name=new_lake_table_name,
                    write_mode=write_mode,
                    lakehouse=lakehouse_id,
                    workspace=lakehouse_workspace_id,
                )

    model_reduced = False
    if perspective is not None:
        from sempy_labs.tom import connect_semantic_model

        with connect_semantic_model(
            dataset=source_dataset, workspace=source_workspace, readonly=True
        ) as tom:
            perspectives = [p.Name for p in tom.model.Perspectives]

            # If invalid perspective, notify user
            if filters is None and perspective in perspectives:
                raise ValueError(
                    f"{icons.red_dot} The '{perspective}' is not a valid perspective in the source semantic model."
                )
            elif filters is not None and perspective not in perspectives:
                print(
                    f"{icons.info} The '{perspective}' is not a valid perspective in the source semantic model."
                )
            # Only reduce model if the perspective does not contain all the objects in the model
            elif not tom._is_perspective_full:
                model_reduced = True
                df_added = tom._reduce_model(perspective_name=perspective)
            bim = tom.get_bim()

    else:
        bim = get_semantic_model_bim(
            dataset=source_dataset, workspace=source_workspace_id
        )

    # Create the semantic model if the model does not exist
    if dfD_filt.empty:
        create_semantic_model_from_bim(
            dataset=target_dataset,
            bim_file=bim,
            workspace=target_workspace_id,
        )
    # Update the semantic model if the model exists
    else:
        update_semantic_model_from_bim(
            dataset=target_dataset, bim_file=bim, workspace=target_workspace_id
        )

    if refresh_target_dataset:
        refresh_semantic_model(dataset=target_dataset, workspace=target_workspace_id)

    if perspective is not None and model_reduced:
        return df_added


@log
def get_semantic_model_bim(
    dataset: str | UUID,
    workspace: Optional[str | UUID] = None,
    save_to_file_name: Optional[str] = None,
) -> dict:
    """
    Extracts the Model.bim file for a given semantic model.

    Parameters
    ----------
    dataset : str | uuid.UUID
        Name or ID of the semantic model.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID in which the semantic model resides.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    save_to_file_name : str, default=None
        If specified, saves the Model.bim as a file in the lakehouse attached to the notebook.

    Returns
    -------
    dict
        The Model.bim file for the semantic model.
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    (dataset_name, dataset_id) = resolve_dataset_name_and_id(dataset, workspace_id)

    bimJson = get_semantic_model_definition(
        dataset=dataset_id,
        workspace=workspace_id,
        format="TMSL",
        return_dataframe=False,
    )

    if save_to_file_name is not None:
        if not lakehouse_attached():
            raise ValueError(
                f"{icons.red_dot} In order to save the model.bim file, a lakehouse must be attached to the notebook. Please attach a lakehouse to this notebook."
            )

        lakehouse = resolve_lakehouse_name()
        folderPath = "/lakehouse/default/Files"
        fileExt = ".bim"
        if not save_to_file_name.endswith(fileExt):
            save_to_file_name = f"{save_to_file_name}{fileExt}"
        filePath = os.path.join(folderPath, save_to_file_name)
        with open(filePath, "w") as json_file:
            json.dump(bimJson, json_file, indent=4)
        print(
            f"{icons.green_dot} The {fileExt} file for the '{dataset_name}' semantic model has been saved to the '{lakehouse}' in this location: '{filePath}'.\n\n"
        )

    return bimJson


@log
def get_semantic_model_definition(
    dataset: str | UUID,
    format: str = "TMSL",
    workspace: Optional[str | UUID] = None,
    return_dataframe: bool = True,
) -> pd.DataFrame | dict | List:
    """
    Extracts the semantic model definition.

    This is a wrapper function for the following API: `Items - Get Semantic Model Definition <https://learn.microsoft.com/rest/api/fabric/semanticmodel/items/get-semantic-model-definition>`_.

    Parameters
    ----------
    dataset : str | uuid.UUID
        Name or ID of the semantic model.
    format : str, default="TMSL"
        The output format. Valid options are "TMSL" or "TMDL". "TMSL" returns the .bim file whereas "TMDL" returns the collection of TMDL files. Can also enter 'bim' for the TMSL version.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID in which the semantic model resides.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    return_dataframe : bool, default=True
        If True, returns a dataframe.
        If False, returns the .bim file for TMSL format. Returns a list of the TMDL files (decoded) for TMDL format.

    Returns
    -------
    pandas.DataFrame | dict | List
        A pandas dataframe with the semantic model definition or the file or files comprising the semantic model definition.
    """

    valid_formats = ["TMSL", "TMDL"]

    format = format.upper()
    if format == "BIM":
        format = "TMSL"
    if format not in valid_formats:
        raise ValueError(
            f"{icons.red_dot} Invalid format. Valid options: {valid_formats}."
        )

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    (dataset_name, dataset_id) = resolve_dataset_name_and_id(dataset, workspace_id)

    result = _base_api(
        request=f"v1/workspaces/{workspace_id}/semanticModels/{dataset_id}/getDefinition?format={format}",
        method="post",
        lro_return_json=True,
        status_codes=None,
    )

    files = result["definition"]["parts"]

    if return_dataframe:
        return pd.json_normalize(files)
    elif format == "TMSL":
        payload = next(
            (part["payload"] for part in files if part["path"] == "model.bim"), None
        )
        return json.loads(_decode_b64(payload))
    else:
        decoded_parts = [
            {"file_name": part["path"], "content": _decode_b64(part["payload"])}
            for part in files
        ]

        return decoded_parts


@log
def get_semantic_model_size(
    dataset: str | UUID, workspace: Optional[str | UUID] = None
):
    """
    Gets size of the semantic model in bytes.

    Parameters
    ----------
    dataset : str | uuid.UUID
        Name or ID of the semantic model.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID in which the semantic model resides.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    int
        The size of the semantic model in
    """

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    (dataset_name, dataset_id) = resolve_dataset_name_and_id(dataset, workspace_id)

    dict = fabric.evaluate_dax(
        dataset=dataset_id,
        workspace=workspace_id,
        dax_string="""
        EVALUATE SELECTCOLUMNS(FILTER(INFO.STORAGETABLECOLUMNS(), [COLUMN_TYPE] = "BASIC_DATA"),[DICTIONARY_SIZE])
        """,
    )

    used_size = fabric.evaluate_dax(
        dataset=dataset_id,
        workspace=workspace_id,
        dax_string="""
        EVALUATE SELECTCOLUMNS(INFO.STORAGETABLECOLUMNSEGMENTS(),[USED_SIZE])
        """,
    )
    dict_size = dict["[DICTIONARY_SIZE]"].sum()
    used_size = used_size["[USED_SIZE]"].sum()
    model_size = dict_size + used_size
    # Calculate proper bytes size by dividing by 1024 and multiplying by 1000 - per 1000
    if model_size >= 10**9:
        result = model_size / (1024**3) * 10**9
    elif model_size >= 10**6:
        result = model_size / (1024**2) * 10**6
    elif model_size >= 10**3:
        result = model_size / (1024) * 10**3

    return result
