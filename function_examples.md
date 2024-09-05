## Function Examples

### [add_user_to_workspace](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.add_user_to_workspace)
#### Adds a user to a workspace.

```python
import sempy_labs as labs
labs.add_user_to_workspace(
    email_address='hello@goodbye.com',
    role_name='',
    principal_type='User', # This parameter is optional
    workspace=None, # This parameter is optional
)
```

### Parameters
> **email_address** (str)
>
>> Required; The email address of the user.
>
> **role_name** (str)
>
>> Required; The [role](https://learn.microsoft.com/rest/api/power-bi/groups/add-group-user#groupuseraccessright) of the user within the workspace.
>
> **principal_type** (str, default='User')
>
>> Optional; The [principal type](https://learn.microsoft.com/rest/api/power-bi/groups/add-group-user#principaltype).
>
> **workspace** (str, default=None)
>
>> Optional; The name of the workspace.
Defaults to None which resolves to the workspace of the attached lakehouse
or if no lakehouse attached, resolves to the workspace of the notebook.
>
### [assign_workspace_to_capacity](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.assign_workspace_to_capacity)
#### Assigns a workspace to a capacity.

```python
import sempy_labs as labs
labs.assign_workspace_to_capacity(
    capacity_name='',
    workspace=None, # This parameter is optional
)
```

### Parameters
> **capacity_name** (str)
>
>> Required; The name of the capacity.
>
> **workspace** (str, default=None)
>
>> Optional; The name of the Fabric workspace.
Defaults to None which resolves to the workspace of the attached lakehouse
or if no lakehouse attached, resolves to the workspace of the notebook.
>
### [assign_workspace_to_dataflow_storage](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.assign_workspace_to_dataflow_storage)
#### Assigns a dataflow storage account to a workspace.

```python
import sempy_labs as labs
labs.assign_workspace_to_dataflow_storage(
    dataflow_storage_account='',
    workspace=None, # This parameter is optional
)
```

### Parameters
> **dataflow_storage_account** (str)
>
>> Required; The name of the dataflow storage account.
>
> **workspace** (str, default=None)
>
>> Optional; The name of the workspace.
Defaults to None which resolves to the workspace of the attached lakehouse
or if no lakehouse attached, resolves to the workspace of the notebook.
>
### [backup_semantic_model](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.backup_semantic_model)
#### [Backs up](https://learn.microsoft.com/azure/analysis-services/analysis-services-backup) a semantic model to the ADLS Gen2 storage account connected to the workspace.

```python
import sempy_labs as labs
labs.backup_semantic_model(
    dataset='AdvWorks',
    file_path='',
    allow_overwrite=True, # This parameter is optional
    apply_compression=True, # This parameter is optional
    workspace=None, # This parameter is optional
)
```

### Parameters
> **dataset** (str)
>
>> Required; Name of the semantic model.
>
> **file_path** (str)
>
>> Required; The ADLS Gen2 storage account location in which to backup the semantic model. Always saves within the 'power-bi-backup/<workspace name>' folder.
Must end in '.abf'.
Example 1: file_path = 'MyModel.abf'
Example 2: file_path = 'MyFolder/MyModel.abf'
>
> **allow_overwrite** (bool, default=True)
>
>> Optional; If True, overwrites backup files of the same name. If False, the file you are saving cannot have the same name as a file that already exists in the same location.
>
> **apply_compression** (bool, default=True)
>
>> Optional; If True, compresses the backup file. Compressed backup files save disk space, but require slightly higher CPU utilization.
>
> **workspace** (str, default=None)
>
>> Optional; The Fabric workspace name.
Defaults to None which resolves to the workspace of the attached lakehouse
or if no lakehouse attached, resolves to the workspace of the notebook.
>
### [cancel_dataset_refresh](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.cancel_dataset_refresh)
#### Cancels the refresh of a semantic model which was executed via the [Enhanced Refresh API](https://learn.microsoft.com/power-bi/connect-data/asynchronous-refresh)

```python
import sempy_labs as labs
labs.cancel_dataset_refresh(
    dataset='AdvWorks',
    request_id=None, # This parameter is optional
    workspace=None, # This parameter is optional
)
```

### Parameters
> **dataset** (str)
>
>> Required; Name of the semantic model.
>
> **request_id** (str, default=None)
>
>> Optional; The request id of a semantic model refresh.
Defaults to finding the latest active refresh of the semantic model.
>
> **workspace** (str, default=None)
>
>> Optional; The Fabric workspace name.
Defaults to None which resolves to the workspace of the attached lakehouse
or if no lakehouse attached, resolves to the workspace of the notebook.
>
### [clear_cache](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.clear_cache)
#### Clears the cache of a semantic model.
See [here](https://learn.microsoft.com/analysis-services/instances/clear-the-analysis-services-caches?view=asallproducts-allversions) for documentation.
```python
import sempy_labs as labs
labs.clear_cache(
    dataset='AdvWorks',
    workspace=None, # This parameter is optional
)
```

### Parameters
> **dataset** (str)
>
>> Required; Name of the semantic model.
>
> **workspace** (str, default=None)
>
>> Optional; The Fabric workspace name.
Defaults to None which resolves to the workspace of the attached lakehouse
or if no lakehouse attached, resolves to the workspace of the notebook.
>
### [commit_to_git](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.commit_to_git)
#### Commits all or a selection of items within a workspace to Git.

```python
import sempy_labs as labs
labs.commit_to_git(
    comment='',
    item_ids=None, # This parameter is optional
    workspace=None, # This parameter is optional
)
```

### Parameters
> **comment** (str)
>
>> Required; The Git commit comment.
>
> **item_ids** (str | List[str], default=None)
>
>> Optional; A list of item Ids to commit to Git.
Defaults to None which commits all items to Git.
>
> **workspace** (str, default=None)
>
>> Optional; The Fabric workspace name.
Defaults to None which resolves to the workspace of the attached lakehouse
or if no lakehouse attached, resolves to the workspace of the notebook.
>
### [connect_workspace_to_git](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.connect_workspace_to_git)
#### Connects a workspace to a git repository.

```python
import sempy_labs as labs
labs.connect_workspace_to_git(
    organization_name='',
    project_name='',
    repository_name='',
    branch_name='',
    directory_name='',
    git_provider_type='AzureDevOps', # This parameter is optional
    workspace=None, # This parameter is optional
)
```

### Parameters
> **organization_name** (str)
>
>> Required; The organization name.
>
> **project_name** (str)
>
>> Required; The project name.
>
> **repository_name** (str)
>
>> Required; The repository name.
>
> **branch_name** (str)
>
>> Required; The branch name.
>
> **directory_name** (str)
>
>> Required; The directory name.
>
> **git_provider_type** (str, default="AzureDevOps")
>
>> Optional; A [Git provider type](https://learn.microsoft.com/rest/api/fabric/core/git/connect?tabs=HTTP#gitprovidertype).
>
> **workspace** (str, default=None)
>
>> Optional; The Fabric workspace name.
Defaults to None which resolves to the workspace of the attached lakehouse
or if no lakehouse attached, resolves to the workspace of the notebook.
>
### [copy_semantic_model_backup_file](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.copy_semantic_model_backup_file)
#### Copies a semantic model backup file (.abf) from an Azure storage account to another location within the Azure storage account.

Requirements:
    1. Must have an Azure storage account and connect it to both the source and target workspace.
    2. Must have an Azure Key Vault.
    3. Must save the Account Key from the Azure storage account as a secret within Azure Key Vault.
```python
import sempy_labs as labs
labs.copy_semantic_model_backup_file(
    source_workspace='',
    target_workspace='',
    source_file_name='',
    target_file_name='',
    storage_account_url='',
    key_vault_uri='',
    key_vault_account_key='',
    source_file_system='power-bi-backup', # This parameter is optional
    target_file_system='power-bi-backup', # This parameter is optional
)
```

### Parameters
> **source_workspace** (str)
>
>> Required; The workspace name of the source semantic model backup file.
>
> **target_workspace** (str)
>
>> Required; The workspace name of the target semantic model backup file destination.
>
> **source_file_name** (str)
>
>> Required; The name of the source backup file (i.e. MyModel.abf).
>
> **target_file_name** (str)
>
>> Required; The name of the target backup file (i.e. MyModel.abf).
>
> **storage_account_url** (str)
>
>> Required; The URL of the storage account. To find this, navigate to the storage account within the Azure Portal. Within 'Endpoints', see the value for the 'Primary Endpoint'.
>
> **key_vault_uri** (str)
>
>> Required; The URI of the Azure Key Vault account.
>
> **key_vault_account_key** (str)
>
>> Required; The key vault secret name which contains the account key of the Azure storage account.
>
> **source_file_system** (str, default="power-bi-backup")
>
>> Optional; The container in which the source backup file is located.
>
> **target_file_system** (str, default="power-bi-backup")
>
>> Optional; The container in which the target backup file will be saved.
>
### [create_abfss_path](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.create_abfss_path)
#### Creates an abfss path for a delta table in a Fabric lakehouse.

```python
import sempy_labs as labs
labs.create_abfss_path(
    lakehouse_id='',
    lakehouse_workspace_id='',
    delta_table_name='',
)
```

### Parameters
> **lakehouse_id** (UUID)
>
>> Required; ID of the Fabric lakehouse.
>
> **lakehouse_workspace_id** (UUID)
>
>> Required; ID of the Fabric workspace.
>
> **delta_table_name** (str)
>
>> Required; Name of the delta table name.
>
### Returns
> str; An abfss path which can be used to save/reference a delta table in a Fabric lakehouse.
### [create_blank_semantic_model](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.create_blank_semantic_model)
#### Creates a new blank semantic model (no tables/columns etc.).

```python
import sempy_labs as labs
labs.create_blank_semantic_model(
    dataset='AdvWorks',
    compatibility_level='1605', # This parameter is optional
    workspace=None, # This parameter is optional
)
```

### Parameters
> **dataset** (str)
>
>> Required; Name of the semantic model.
>
> **compatibility_level** (int, default=1605)
>
>> Optional; The compatibility level of the semantic model.
>
> **workspace** (str, default=None)
>
>> Optional; The Fabric workspace name.
Defaults to None which resolves to the workspace of the attached lakehouse
or if no lakehouse attached, resolves to the workspace of the notebook.
>
### [create_custom_pool](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.create_custom_pool)
#### Creates a [custom pool](https://learn.microsoft.com/fabric/data-engineering/create-custom-spark-pools) within a workspace.

```python
import sempy_labs as labs
labs.create_custom_pool(
    pool_name='',
    node_size='',
    min_node_count='',
    max_node_count='',
    min_executors='',
    max_executors='',
    node_family='MemoryOptimized', # This parameter is optional
    auto_scale_enabled=True, # This parameter is optional
    dynamic_executor_allocation_enabled=True, # This parameter is optional
    workspace=None, # This parameter is optional
)
```

### Parameters
> **pool_name** (str)
>
>> Required; The custom pool name.
>
> **node_size** (str)
>
>> Required; The [node size](https://learn.microsoft.com/rest/api/fabric/spark/custom-pools/create-workspace-custom-pool?tabs=HTTP#nodesize).
>
> **min_node_count** (int)
>
>> Required; The [minimum node count](https://learn.microsoft.com/rest/api/fabric/spark/custom-pools/create-workspace-custom-pool?tabs=HTTP#autoscaleproperties).
>
> **max_node_count** (int)
>
>> Required; The [maximum node count](https://learn.microsoft.com/en-us/rest/api/fabric/spark/custom-pools/create-workspace-custom-pool?tabs=HTTP#autoscaleproperties).
>
> **min_executors** (int)
>
>> Required; The [minimum executors](https://learn.microsoft.com/rest/api/fabric/spark/custom-pools/create-workspace-custom-pool?tabs=HTTP#dynamicexecutorallocationproperties).
>
> **max_executors** (int)
>
>> Required; The [maximum executors](https://learn.microsoft.com/en-us/rest/api/fabric/spark/custom-pools/create-workspace-custom-pool?tabs=HTTP#dynamicexecutorallocationproperties).
>
> **node_family** (str, default='MemoryOptimized')
>
>> Optional; The [node family](https://learn.microsoft.com/rest/api/fabric/spark/custom-pools/create-workspace-custom-pool?tabs=HTTP#nodefamily).
>
> **auto_scale_enabled** (bool, default=True)
>
>> Optional; The status of [auto scale](https://learn.microsoft.com/rest/api/fabric/spark/custom-pools/create-workspace-custom-pool?tabs=HTTP#autoscaleproperties).
>
> **dynamic_executor_allocation_enabled** (bool, default=True)
>
>> Optional; The status of the [dynamic executor allocation](https://learn.microsoft.com/en-us/rest/api/fabric/spark/custom-pools/create-workspace-custom-pool?tabs=HTTP#dynamicexecutorallocationproperties).
>
> **workspace** (str, default=None)
>
>> Optional; The name of the Fabric workspace.
Defaults to None which resolves to the workspace of the attached lakehouse
or if no lakehouse attached, resolves to the workspace of the notebook.
>
### [create_model_bpa_semantic_model](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.create_model_bpa_semantic_model)
#### Dynamically generates a Direct Lake semantic model based on the 'modelbparesults' delta table which contains the Best Practice Analyzer results.
This semantic model used in combination with the corresponding Best Practice Analyzer report can be used to analyze multiple semantic models
on multiple workspaces at once (and over time).

The semantic model is always created within the same workspace as the lakehouse.
```python
import sempy_labs as labs
labs.create_model_bpa_semantic_model(
    dataset='ModelBPA', # This parameter is optional
    lakehouse=None, # This parameter is optional
    lakehouse_workspace=None, # This parameter is optional
)
```

### Parameters
> **dataset** (str, default='ModelBPA')
>
>> Optional; Name of the semantic model to be created.
>
> **lakehouse** (str, default=None)
>
>> Optional; Name of the Fabric lakehouse which contains the 'modelbparesults' delta table.
Defaults to None which resolves to the default lakehouse attached to the notebook.
>
> **lakehouse_workspace** (str, default=None)
>
>> Optional; The workspace in which the lakehouse resides.
Defaults to None which resolves to the workspace of the attached lakehouse
or if no lakehouse attached, resolves to the workspace of the notebook.
>
### [create_relationship_name](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.create_relationship_name)
#### Formats a relationship's table/columns into a fully qualified name.

```python
import sempy_labs as labs
labs.create_relationship_name(
    from_table='',
    from_column='',
    to_table='',
    to_column='',
)
```

### Parameters
> **from_table** (str)
>
>> Required; The name of the table on the 'from' side of the relationship.
>
> **from_column** (str)
>
>> Required; The name of the column on the 'from' side of the relationship.
>
> **to_table** (str)
>
>> Required; The name of the table on the 'to' side of the relationship.
>
> **to_column** (str)
>
>> Required; The name of the column on the 'to' side of the relationship.
>
### Returns
> str; The fully qualified relationship name.
### [create_semantic_model_from_bim](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.create_semantic_model_from_bim)
#### Creates a new semantic model based on a Model.bim file.

```python
import sempy_labs as labs
labs.create_semantic_model_from_bim(
    dataset='AdvWorks',
    bim_file='',
    workspace=None, # This parameter is optional
)
```

### Parameters
> **dataset** (str)
>
>> Required; Name of the semantic model.
>
> **bim_file** (dict)
>
>> Required; The model.bim file.
>
> **workspace** (str, default=None)
>
>> Optional; The Fabric workspace name.
Defaults to None which resolves to the workspace of the attached lakehouse
or if no lakehouse attached, resolves to the workspace of the notebook.
>
### [create_warehouse](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.create_warehouse)
#### Creates a Fabric warehouse.

```python
import sempy_labs as labs
labs.create_warehouse(
    warehouse='',
    description=None, # This parameter is optional
    workspace=None, # This parameter is optional
)
```

### Parameters
> **warehouse** (str)
>
>> Required; Name of the warehouse.
>
> **description** (str, default=None)
>
>> Optional; A description of the warehouse.
>
> **workspace** (str, default=None)
>
>> Optional; The Fabric workspace name.
Defaults to None which resolves to the workspace of the attached lakehouse
or if no lakehouse attached, resolves to the workspace of the notebook.
>
### [delete_custom_pool](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.delete_custom_pool)
#### Deletes a [custom pool](https://learn.microsoft.com/fabric/data-engineering/create-custom-spark-pools) within a workspace.

```python
import sempy_labs as labs
labs.delete_custom_pool(
    pool_name='',
    workspace=None, # This parameter is optional
)
```

### Parameters
> **pool_name** (str)
>
>> Required; The custom pool name.
>
> **workspace** (str, default=None)
>
>> Optional; The name of the Fabric workspace.
Defaults to None which resolves to the workspace of the attached lakehouse
or if no lakehouse attached, resolves to the workspace of the notebook.
>
### [delete_user_from_workspace](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.delete_user_from_workspace)
#### Removes a user from a workspace.

```python
import sempy_labs as labs
labs.delete_user_from_workspace(
    email_address='hello@goodbye.com',
    workspace=None, # This parameter is optional
)
```

### Parameters
> **email_address** (str)
>
>> Required; The email address of the user.
>
> **workspace** (str, default=None)
>
>> Optional; The name of the workspace.
Defaults to None which resolves to the workspace of the attached lakehouse
or if no lakehouse attached, resolves to the workspace of the notebook.
>
### [deploy_semantic_model](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.deploy_semantic_model)
#### Deploys a semantic model based on an existing semantic model.

```python
import sempy_labs as labs
labs.deploy_semantic_model(
    source_dataset='',
    source_workspace=None, # This parameter is optional
    target_dataset=None, # This parameter is optional
    target_workspace=None, # This parameter is optional
    refresh_target_dataset=True, # This parameter is optional
)
```

### Parameters
> **source_dataset** (str)
>
>> Required; Name of the semantic model to deploy.
>
> **source_workspace** (str, default=None)
>
>> Optional; The Fabric workspace name.
Defaults to None which resolves to the workspace of the attached lakehouse
or if no lakehouse attached, resolves to the workspace of the notebook.
>
> **target_dataset** (str)
>
>> Optional; Name of the new semantic model to be created.
>
> **target_workspace** (str, default=None)
>
>> Optional; The Fabric workspace name in which the new semantic model will be deployed.
Defaults to None which resolves to the workspace of the attached lakehouse
or if no lakehouse attached, resolves to the workspace of the notebook.
>
> **refresh_target_dataset** (bool, default=True)
>
>> Optional; If set to True, this will initiate a full refresh of the target semantic model in the target workspace.
>
### [deprovision_workspace_identity](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.deprovision_workspace_identity)
#### Deprovisions a workspace identity for a workspace.

```python
import sempy_labs as labs
labs.deprovision_workspace_identity(
    workspace=None, # This parameter is optional
)
```

### Parameters
> **workspace** (str, default=None)
>
>> Optional; The Fabric workspace name.
Defaults to None which resolves to the workspace of the attached lakehouse
or if no lakehouse attached, resolves to the workspace of the notebook.
>
### [disable_qso](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.disable_qso)
#### Sets the max read-only replicas to 0, disabling query scale out.

```python
import sempy_labs as labs
labs.disable_qso(
    dataset='AdvWorks',
    workspace=None, # This parameter is optional
)
```

### Parameters
> **dataset** (str)
>
>> Required; Name of the semantic model.
>
> **workspace** (str, default=None)
>
>> Optional; The Fabric workspace name.
Defaults to None which resolves to the workspace of the attached lakehouse
or if no lakehouse attached, resolves to the workspace of the notebook.
>
### Returns
> pandas.DataFrame; A pandas dataframe showing the current query scale out settings.
### [disconnect_workspace_from_git](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.disconnect_workspace_from_git)
#### Disconnects a workpsace from a git repository.

```python
import sempy_labs as labs
labs.disconnect_workspace_from_git(
    workspace=None, # This parameter is optional
)
```

### Parameters
> **workspace** (str, default=None)
>
>> Optional; The Fabric workspace name.
Defaults to None which resolves to the workspace of the attached lakehouse
or if no lakehouse attached, resolves to the workspace of the notebook.
>
### [evaluate_dax_impersonation](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.evaluate_dax_impersonation)
#### Runs a DAX query against a semantic model using the [REST API](https://learn.microsoft.com/en-us/rest/api/power-bi/datasets/execute-queries-in-group).

Compared to evaluate_dax this allows passing the user name for impersonation.
Note that the REST API has significant limitations compared to the XMLA endpoint.
```python
import sempy_labs as labs
labs.evaluate_dax_impersonation(
    dataset='AdvWorks',
    dax_query='EVALUATE SUMMARIZECOLUMNS("MyMeasure", 1)',
    user_name=None, # This parameter is optional
    workspace=None, # This parameter is optional
)
```

### Parameters
> **dataset** (str)
>
>> Required; Name of the semantic model.
>
> **dax_query** (str)
>
>> Required; The DAX query.
>
> **user_name** (str)
>
>> Optional; The user name (i.e. hello@goodbye.com).
>
> **workspace** (str, default=None)
>
>> Optional; The Fabric workspace name.
Defaults to None which resolves to the workspace of the attached lakehouse
or if no lakehouse attached, resolves to the workspace of the notebook.
>
### Returns
> pandas.DataFrame; A pandas dataframe holding the result of the DAX query.
### [export_model_to_onelake](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.export_model_to_onelake)
#### Exports a semantic model's tables to delta tables in the lakehouse. Creates shortcuts to the tables if a lakehouse is specified.

```python
import sempy_labs as labs
labs.export_model_to_onelake(
    dataset='AdvWorks',
    workspace=None, # This parameter is optional
    destination_lakehouse=None, # This parameter is optional
    destination_workspace=None, # This parameter is optional
)
```

### Parameters
> **dataset** (str)
>
>> Required; Name of the semantic model.
>
> **workspace** (str, default=None)
>
>> Optional; The Fabric workspace name.
Defaults to None which resolves to the workspace of the attached lakehouse
or if no lakehouse attached, resolves to the workspace of the notebook.
>
> **destination_lakehouse** (str, default=None)
>
>> Optional; The name of the Fabric lakehouse where shortcuts will be created to access the delta tables created by the export. If the lakehouse specified does not exist, one will be created with that name. If no lakehouse is specified, shortcuts will not be created.
>
> **destination_workspace** (str, default=None)
>
>> Optional; The name of the Fabric workspace in which the lakehouse resides.
>
### [format_dax_object_name](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.format_dax_object_name)
#### Formats a table/column combination to the 'Table Name'[Column Name] format.

```python
import sempy_labs as labs
labs.format_dax_object_name(
    table='',
    column=tom.model.Tables["Geography"].Columns["GeographyKey"],
)
```

### Parameters
> **table** (str)
>
>> Required; The name of the table.
>
> **column** (str)
>
>> Required; The name of the column.
>
### Returns
> str; The fully qualified object name.
### [generate_embedded_filter](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.generate_embedded_filter)
#### Converts the filter expression to a filter expression which can be used by a Power BI embedded URL.

```python
import sempy_labs as labs
labs.generate_embedded_filter(
    filter='',
)
```

### Parameters
> **filter** (str)
>
>> Required; The filter expression for an embedded Power BI report.
>
### Returns
> str; A filter expression usable by a Power BI embedded URL.
### [get_capacity_id](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.get_capacity_id)
#### Obtains the Capacity Id for a given workspace.

```python
import sempy_labs as labs
labs.get_capacity_id(
    workspace=None, # This parameter is optional
)
```

### Parameters
> **workspace** (str, default=None)
>
>> Optional; The Fabric workspace name.
Defaults to None which resolves to the workspace of the attached lakehouse
or if no lakehouse attached, resolves to the workspace of the notebook.
>
### Returns
> UUID; The capacity Id.
### [get_capacity_name](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.get_capacity_name)
#### Obtains the capacity name for a given workspace.

```python
import sempy_labs as labs
labs.get_capacity_name(
    workspace=None, # This parameter is optional
)
```

### Parameters
> **workspace** (str, default=None)
>
>> Optional; The Fabric workspace name.
Defaults to None which resolves to the workspace of the attached lakehouse
or if no lakehouse attached, resolves to the workspace of the notebook.
>
### Returns
> str; The capacity name.
### [get_direct_lake_sql_endpoint](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.get_direct_lake_sql_endpoint)
#### Obtains the SQL Endpoint ID of the semantic model.

```python
import sempy_labs as labs
labs.get_direct_lake_sql_endpoint(
    dataset='AdvWorks',
    workspace=None, # This parameter is optional
)
```

### Parameters
> **dataset** (str)
>
>> Required; The name of the semantic model.
>
> **workspace** (str, default=None)
>
>> Optional; The Fabric workspace name.
Defaults to None which resolves to the workspace of the attached lakehouse
or if no lakehouse attached, resolves to the workspace of the notebook.
>
### Returns
> uuid.UUID; The ID of SQL Endpoint.
### [get_git_connection](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.get_git_connection)
#### Obtains the Git status of items in the workspace, that can be committed to Git.

```python
import sempy_labs as labs
labs.get_git_connection(
    workspace=None, # This parameter is optional
)
```

### Parameters
> **workspace** (str, default=None)
>
>> Optional; The Fabric workspace name.
Defaults to None which resolves to the workspace of the attached lakehouse
or if no lakehouse attached, resolves to the workspace of the notebook.
>
### Returns
> pandas.DataFrame; A pandas dataframe showing the Git status of items in the workspace.
### [get_git_status](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.get_git_status)
#### Obtains the Git status of items in the workspace, that can be committed to Git.

```python
import sempy_labs as labs
labs.get_git_status(
    workspace=None, # This parameter is optional
)
```

### Parameters
> **workspace** (str, default=None)
>
>> Optional; The Fabric workspace name.
Defaults to None which resolves to the workspace of the attached lakehouse
or if no lakehouse attached, resolves to the workspace of the notebook.
>
### Returns
> pandas.DataFrame; A pandas dataframe showing the Git status of items in the workspace.
### [get_measure_dependencies](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.get_measure_dependencies)
#### Shows all dependencies for all measures in a semantic model.

```python
import sempy_labs as labs
labs.get_measure_dependencies(
    dataset='AdvWorks',
    workspace=None, # This parameter is optional
)
```

### Parameters
> **dataset** (str)
>
>> Required; Name of the semantic model.
>
> **workspace** (str, default=None)
>
>> Optional; The Fabric workspace name.
Defaults to None which resolves to the workspace of the attached lakehouse
or if no lakehouse attached, resolves to the workspace of the notebook.
>
### Returns
> pandas.DataFrame; Shows all dependencies for all measures in the semantic model.
### [get_model_calc_dependencies](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.get_model_calc_dependencies)
#### Shows all dependencies for all objects in a semantic model.

```python
import sempy_labs as labs
labs.get_model_calc_dependencies(
    dataset='AdvWorks',
    workspace=None, # This parameter is optional
)
```

### Parameters
> **dataset** (str)
>
>> Required; Name of the semantic model.
>
> **workspace** (str, default=None)
>
>> Optional; The Fabric workspace name.
Defaults to None which resolves to the workspace of the attached lakehouse
or if no lakehouse attached, resolves to the workspace of the notebook.
>
### Returns
> pandas.DataFrame; Shows all dependencies for all objects in the semantic model.
### [get_notebook_definition](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.get_notebook_definition)
#### Obtains the notebook definition.

```python
import sempy_labs as labs
labs.get_notebook_definition(
    notebook_name='',
    workspace=None, # This parameter is optional
    decode=True, # This parameter is optional
)
```

### Parameters
> **notebook_name** (str)
>
>> Required; The name of the notebook.
>
> **workspace** (str, default=None)
>
>> Optional; The name of the workspace.
Defaults to None which resolves to the workspace of the attached lakehouse
or if no lakehouse attached, resolves to the workspace of the notebook.
>
> **decode** (bool, default=True)
>
>> Optional; If True, decodes the notebook definition file into .ipynb format.
If False, obtains the notebook definition file in base64 format.
>
### Returns
> ipynb; The notebook definition.
### [get_object_level_security](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.get_object_level_security)
#### Shows the object level security for the semantic model.

```python
import sempy_labs as labs
labs.get_object_level_security(
    dataset='AdvWorks',
    workspace=None, # This parameter is optional
)
```

### Parameters
> **dataset** (str)
>
>> Required; Name of the semantic model.
>
> **workspace** (str, default=None)
>
>> Optional; The Fabric workspace name.
Defaults to None which resolves to the workspace of the attached lakehouse
or if no lakehouse attached, resolves to the workspace of the notebook.
>
### Returns
> pandas.DataFrame; A pandas dataframe showing the object level security for the semantic model.
### [get_semantic_model_bim](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.get_semantic_model_bim)
#### Extracts the Model.bim file for a given semantic model.

```python
import sempy_labs as labs
labs.get_semantic_model_bim(
    dataset='AdvWorks',
    workspace=None, # This parameter is optional
    save_to_file_name=None, # This parameter is optional
    lakehouse_workspace=None, # This parameter is optional
)
```

### Parameters
> **dataset** (str)
>
>> Required; Name of the semantic model.
>
> **workspace** (str, default=None)
>
>> Optional; The Fabric workspace name in which the semantic model resides.
Defaults to None which resolves to the workspace of the attached lakehouse
or if no lakehouse attached, resolves to the workspace of the notebook.
>
> **save_to_file_name** (str, default=None)
>
>> Optional; If specified, saves the Model.bim as a file in the lakehouse attached to the notebook.
>
> **lakehouse_workspace** (str, default=None)
>
>> Optional; The Fabric workspace name in which the lakehouse attached to the workspace resides.
Defaults to None which resolves to the workspace of the attached lakehouse
or if no lakehouse attached, resolves to the workspace of the notebook.
>
### Returns
> dict; The Model.bim file for the semantic model.
### [get_spark_settings](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.get_spark_settings)
#### Shows the spark settings for a workspace.

```python
import sempy_labs as labs
labs.get_spark_settings(
    workspace=None, # This parameter is optional
)
```

### Parameters
> **workspace** (str, default=None)
>
>> Optional; The name of the Fabric workspace.
Defaults to None which resolves to the workspace of the attached lakehouse
or if no lakehouse attached, resolves to the workspace of the notebook.
>
### Returns
> pandas.DataFrame; A pandas dataframe showing the spark settings for a workspace.
### [import_notebook_from_web](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.import_notebook_from_web)
#### Creates a new notebook within a workspace based on a Jupyter notebook hosted in the web.

```python
import sempy_labs as labs
labs.import_notebook_from_web(
    notebook_name='',
    url='',
    description=None, # This parameter is optional
    workspace=None, # This parameter is optional
)
```

### Parameters
> **notebook_name** (str)
>
>> Required; The name of the notebook to be created.
>
> **url** (str)
>
>> Required; The url of the Jupyter Notebook (.ipynb)
>
> **description** (str, default=None)
>
>> Optional; The description of the notebook.
Defaults to None which does not place a description.
>
> **workspace** (str, default=None)
>
>> Optional; The name of the workspace.
Defaults to None which resolves to the workspace of the attached lakehouse
or if no lakehouse attached, resolves to the workspace of the notebook.
>
### [import_vertipaq_analyzer](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.import_vertipaq_analyzer)
#### Imports and visualizes the vertipaq analyzer info from a saved .zip file in your lakehouse.

```python
import sempy_labs as labs
labs.import_vertipaq_analyzer(
    folder_path='',
    file_name='',
)
```

### Parameters
> **folder_path** (str)
>
>> Required; The folder within your lakehouse in which the .zip file containing the vertipaq analyzer info has been saved.
>
> **file_name** (str)
>
>> Required; The file name of the file which contains the vertipaq analyzer info.
>
### Returns
> str; A visualization of the Vertipaq Analyzer statistics.
### [initialize_git_connection](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.initialize_git_connection)
#### Initializes a connection for a workspace that is connected to Git.

```python
import sempy_labs as labs
labs.initialize_git_connection(
    workspace=None, # This parameter is optional
)
```

### Parameters
> **workspace** (str, default=None)
>
>> Optional; The Fabric workspace name.
Defaults to None which resolves to the workspace of the attached lakehouse
or if no lakehouse attached, resolves to the workspace of the notebook.
>
### [is_default_semantic_model](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.is_default_semantic_model)
#### Identifies whether a semantic model is a default semantic model.

```python
import sempy_labs as labs
labs.is_default_semantic_model(
    dataset='AdvWorks',
    workspace=None, # This parameter is optional
)
```

### Parameters
> **dataset** (str)
>
>> Required; The name of the semantic model.
>
> **workspace** (str, default=None)
>
>> Optional; The Fabric workspace name.
Defaults to None which resolves to the workspace of the attached lakehouse
or if no lakehouse attached, resolves to the workspace of the notebook.
>
### Returns
> bool; A True/False value indicating whether the semantic model is a default semantic model.
### [list_capacities](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.list_capacities)
#### Shows the capacities and their properties.

```python
import sempy_labs as labs
labs.list_capacities()
```

### Returns
> pandas.DataFrame; A pandas dataframe showing the capacities and their properties
### [list_custom_pools](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.list_custom_pools)
#### Lists all [custom pools](https://learn.microsoft.com/fabric/data-engineering/create-custom-spark-pools) within a workspace.

```python
import sempy_labs as labs
labs.list_custom_pools(
    workspace=None, # This parameter is optional
)
```

### Parameters
> **workspace** (str, default=None)
>
>> Optional; The name of the Fabric workspace.
Defaults to None which resolves to the workspace of the attached lakehouse
or if no lakehouse attached, resolves to the workspace of the notebook.
>
### Returns
> pandas.DataFrame; A pandas dataframe showing all the custom pools within the Fabric workspace.
### [list_dashboards](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.list_dashboards)
#### Shows a list of the dashboards within a workspace.

```python
import sempy_labs as labs
labs.list_dashboards(
    workspace=None, # This parameter is optional
)
```

### Parameters
> **workspace** (str, default=None)
>
>> Optional; The Fabric workspace name.
Defaults to None which resolves to the workspace of the attached lakehouse
or if no lakehouse attached, resolves to the workspace of the notebook.
>
### Returns
> pandas.DataFrame; A pandas dataframe showing the dashboards within a workspace.
### [list_dataflow_storage_accounts](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.list_dataflow_storage_accounts)
#### Shows the accessible dataflow storage accounts.

```python
import sempy_labs as labs
labs.list_dataflow_storage_accounts()
```

### Returns
> pandas.DataFrame; A pandas dataframe showing the accessible dataflow storage accounts.
### [list_dataflows](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.list_dataflows)
#### Shows a list of all dataflows which exist within a workspace.

```python
import sempy_labs as labs
labs.list_dataflows(
    workspace=None, # This parameter is optional
)
```

### Parameters
> **workspace** (str, default=None)
>
>> Optional; The Fabric workspace name.
Defaults to None which resolves to the workspace of the attached lakehouse
or if no lakehouse attached, resolves to the workspace of the notebook.
>
### Returns
> pandas.DataFrame; A pandas dataframe showing the dataflows which exist within a workspace.
### [list_deployment_pipeline_stage_items](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.list_deployment_pipeline_stage_items)
#### Shows the supported items from the workspace assigned to the specified stage of the specified deployment pipeline.

```python
import sempy_labs as labs
labs.list_deployment_pipeline_stage_items(
    deployment_pipeline='',
    stage_name='',
)
```

### Parameters
> **deployment_pipeline** (str)
>
>> Required; The deployment pipeline name.
>
> **stage_name** (str)
>
>> Required; The deployment pipeline stage name.
>
### Returns
> pandas.DataFrame; A pandas dataframe showing the supported items from the workspace assigned to the specified stage of the specified deployment pipeline.
### [list_deployment_pipeline_stages](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.list_deployment_pipeline_stages)
#### Shows the specified deployment pipeline stages.

```python
import sempy_labs as labs
labs.list_deployment_pipeline_stages(
    deployment_pipeline='',
)
```

### Parameters
> **deployment_pipeline** (str)
>
>> Required; The deployment pipeline name.
>
### Returns
> pandas.DataFrame; A pandas dataframe showing the specified deployment pipeline stages.
### [list_deployment_pipelines](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.list_deployment_pipelines)
#### Shows a list of deployment pipelines the user can access.

```python
import sempy_labs as labs
labs.list_deployment_pipelines()
```

### Returns
> pandas.DataFrame; A pandas dataframe showing a list of deployment pipelines the user can access.
### [list_lakehouses](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.list_lakehouses)
#### Shows the lakehouses within a workspace.

```python
import sempy_labs as labs
labs.list_lakehouses(
    workspace=None, # This parameter is optional
)
```

### Parameters
> **workspace** (str, default=None)
>
>> Optional; The Fabric workspace name.
Defaults to None which resolves to the workspace of the attached lakehouse
or if no lakehouse attached, resolves to the workspace of the notebook.
>
### Returns
> pandas.DataFrame; A pandas dataframe showing the lakehouses within a workspace.
### [list_qso_settings](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.list_qso_settings)
#### Shows the query scale out settings for a semantic model (or all semantic models within a workspace).

```python
import sempy_labs as labs
labs.list_qso_settings(
    dataset=None, # This parameter is optional
    workspace=None, # This parameter is optional
)
```

### Parameters
> **dataset** (str, default=None)
>
>> Optional; Name of the semantic model.
>
> **workspace** (str, default=None)
>
>> Optional; The Fabric workspace name.
Defaults to None which resolves to the workspace of the attached lakehouse
or if no lakehouse attached, resolves to the workspace of the notebook.
>
### Returns
> pandas.DataFrame; A pandas dataframe showing the query scale out settings.
### [list_reports_using_semantic_model](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.list_reports_using_semantic_model)
#### Shows a list of all the reports (in all workspaces) which use a given semantic model.

```python
import sempy_labs as labs
labs.list_reports_using_semantic_model(
    dataset='AdvWorks',
    workspace=None, # This parameter is optional
)
```

### Parameters
> **dataset** (str)
>
>> Required; Name of the semantic model.
>
> **workspace** (str, default=None)
>
>> Optional; The Fabric workspace name.
Defaults to None which resolves to the workspace of the attached lakehouse
or if no lakehouse attached, resolves to the workspace of the notebook.
>
### Returns
> pandas.DataFrame; A pandas dataframe showing the reports which use a given semantic model.
### [list_semantic_model_objects](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.list_semantic_model_objects)
#### Shows a list of semantic model objects.

```python
import sempy_labs as labs
labs.list_semantic_model_objects(
    dataset='AdvWorks',
    workspace=None, # This parameter is optional
)
```

### Parameters
> **dataset** (str)
>
>> Required; Name of the semantic model.
>
> **workspace** (str, default=None)
>
>> Optional; The Fabric workspace name.
Defaults to None which resolves to the workspace of the attached lakehouse
or if no lakehouse attached, resolves to the workspace of the notebook.
>
### Returns
> pandas.DataFrame; A pandas dataframe showing a list of objects in the semantic model
### [list_shortcuts](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.list_shortcuts)
#### Shows all shortcuts which exist in a Fabric lakehouse and their properties.

```python
import sempy_labs as labs
labs.list_shortcuts(
    lakehouse=None, # This parameter is optional
    workspace=None, # This parameter is optional
)
```

### Parameters
> **lakehouse** (str, default=None)
>
>> Optional; The Fabric lakehouse name.
Defaults to None which resolves to the lakehouse attached to the notebook.
>
> **workspace** (str, default=None)
>
>> Optional; The name of the Fabric workspace in which lakehouse resides.
Defaults to None which resolves to the workspace of the attached lakehouse
or if no lakehouse attached, resolves to the workspace of the notebook.
>
### Returns
> pandas.DataFrame; A pandas dataframe showing all the shortcuts which exist in the specified lakehouse.
### [list_warehouses](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.list_warehouses)
#### Shows the warehouses within a workspace.

```python
import sempy_labs as labs
labs.list_warehouses(
    workspace=None, # This parameter is optional
)
```

### Parameters
> **workspace** (str, default=None)
>
>> Optional; The Fabric workspace name.
Defaults to None which resolves to the workspace of the attached lakehouse
or if no lakehouse attached, resolves to the workspace of the notebook.
>
### Returns
> pandas.DataFrame; A pandas dataframe showing the warehouses within a workspace.
### [list_workspace_role_assignments](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.list_workspace_role_assignments)
#### Shows the members of a given workspace.

```python
import sempy_labs as labs
labs.list_workspace_role_assignments(
    workspace=None, # This parameter is optional
)
```

### Parameters
> **workspace** (str, default=None)
>
>> Optional; The Fabric workspace name.
Defaults to None which resolves to the workspace of the attached lakehouse
or if no lakehouse attached, resolves to the workspace of the notebook.
>
### Returns
> pandas.DataFrame; A pandas dataframe showing the members of a given workspace and their roles.
### [list_workspace_users](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.list_workspace_users)
#### A list of all the users of a workspace and their roles.

```python
import sempy_labs as labs
labs.list_workspace_users(
    workspace=None, # This parameter is optional
)
```

### Parameters
> **workspace** (str, default=None)
>
>> Optional; The name of the workspace.
Defaults to None which resolves to the workspace of the attached lakehouse
or if no lakehouse attached, resolves to the workspace of the notebook.
>
### Returns
> pandas.DataFrame; A pandas dataframe the users of a workspace and their properties.
### [measure_dependency_tree](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.measure_dependency_tree)
#### Prints a measure dependency tree of all dependent objects for a measure in a semantic model.

```python
import sempy_labs as labs
labs.measure_dependency_tree(
    dataset='AdvWorks',
    measure_name='',
    workspace=None, # This parameter is optional
)
```

### Parameters
> **dataset** (str)
>
>> Required; Name of the semantic model.
>
> **measure_name** (str)
>
>> Required; Name of the measure.
>
> **workspace** (str, default=None)
>
>> Optional; The Fabric workspace name.
Defaults to None which resolves to the workspace of the attached lakehouse
or if no lakehouse attached, resolves to the workspace of the notebook.
>
### [model_bpa_rules](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.model_bpa_rules)
#### Shows the default rules for the semantic model BPA used by the run_model_bpa function.

```python
import sempy_labs as labs
labs.model_bpa_rules(
    dataset='AdvWorks',
    workspace=None, # This parameter is optional
    dependencies=None, # This parameter is optional
)
```

### Parameters
> **dataset** (str)
>
>> Required; Name of the semantic model.
>
> **workspace** (str, default=None)
>
>> Optional; The Fabric workspace name.
Defaults to None which resolves to the workspace of the attached lakehouse
or if no lakehouse attached, resolves to the workspace of the notebook.
>
> **dependencies** (pd.DataFrame, default=None)
>
>> Optional; A pandas dataframe with the output of the 'get_model_calc_dependencies' function.
>
### Returns
> pandas.DataFrame; A pandas dataframe containing the default rules for the run_model_bpa function.
### [provision_workspace_identity](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.provision_workspace_identity)
#### Provisions a workspace identity for a workspace.

```python
import sempy_labs as labs
labs.provision_workspace_identity(
    workspace=None, # This parameter is optional
)
```

### Parameters
> **workspace** (str, default=None)
>
>> Optional; The Fabric workspace name.
Defaults to None which resolves to the workspace of the attached lakehouse
or if no lakehouse attached, resolves to the workspace of the notebook.
>
### [qso_sync](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.qso_sync)
#### Triggers a query scale-out sync of read-only replicas for the specified dataset from the specified workspace.

```python
import sempy_labs as labs
labs.qso_sync(
    dataset='AdvWorks',
    workspace=None, # This parameter is optional
)
```

### Parameters
> **dataset** (str)
>
>> Required; Name of the semantic model.
>
> **workspace** (str, default=None)
>
>> Optional; The Fabric workspace name.
Defaults to None which resolves to the workspace of the attached lakehouse
or if no lakehouse attached, resolves to the workspace of the notebook.
>
### [qso_sync_status](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.qso_sync_status)
#### Returns the query scale-out sync status for the specified dataset from the specified workspace.

```python
import sempy_labs as labs
labs.qso_sync_status(
    dataset='AdvWorks',
    workspace=None, # This parameter is optional
)
```

### Parameters
> **dataset** (str)
>
>> Required; Name of the semantic model.
>
> **workspace** (str, default=None)
>
>> Optional; The Fabric workspace name.
Defaults to None which resolves to the workspace of the attached lakehouse
or if no lakehouse attached, resolves to the workspace of the notebook.
>
### Returns
> Tuple[pandas.DataFrame, pandas.DataFrame]; 2 pandas dataframes showing the query scale-out sync status.
### [refresh_semantic_model](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.refresh_semantic_model)
#### Refreshes a semantic model.

```python
import sempy_labs as labs
labs.refresh_semantic_model(
    dataset='AdvWorks',
    tables=None, # This parameter is optional
    partitions=None, # This parameter is optional
    refresh_type=None, # This parameter is optional
    retry_count=0, # This parameter is optional
    apply_refresh_policy=True, # This parameter is optional
    max_parallelism='10', # This parameter is optional
    workspace=None, # This parameter is optional
)
```

### Parameters
> **dataset** (str)
>
>> Required; Name of the semantic model.
>
> **tables** (str, List[str], default=None)
>
>> Optional; A string or a list of tables to refresh.
>
> **partitions** (str, List[str], default=None)
>
>> Optional; A string or a list of partitions to refresh. Partitions must be formatted as such: 'Table Name'[Partition Name].
>
> **refresh_type** (str, default='full')
>
>> Optional; The type of processing to perform. Types align with the TMSL refresh command types: full, clearValues, calculate, dataOnly, automatic, and defragment. The add type isn't supported. Defaults to "full".
>
> **retry_count** (int, default=0)
>
>> Optional; Number of times the operation retries before failing.
>
> **apply_refresh_policy** (bool, default=True)
>
>> Optional; If an incremental refresh policy is defined, determines whether to apply the policy. Modes are true or false. If the policy isn't applied, the full process leaves partition definitions unchanged, and fully refreshes all partitions in the table. If commitMode is transactional, applyRefreshPolicy can be true or false. If commitMode is partialBatch, applyRefreshPolicy of true isn't supported, and applyRefreshPolicy must be set to false.
>
> **max_parallelism** (int, default=10)
>
>> Optional; Determines the maximum number of threads that can run the processing commands in parallel.
This value aligns with the MaxParallelism property that can be set in the TMSL Sequence command or by using other methods.
Defaults to 10.
>
> **workspace** (str, default=None)
>
>> Optional; The Fabric workspace name.
Defaults to None which resolves to the workspace of the attached lakehouse
or if no lakehouse attached, resolves to the workspace of the notebook.
>
### [resolve_capacity_name](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.resolve_capacity_name)
#### Obtains the capacity name for a given capacity Id.

```python
import sempy_labs as labs
labs.resolve_capacity_name(
    capacity_id=None, # This parameter is optional
)
```

### Parameters
> **capacity_id** (UUID, default=None)
>
>> Optional; The capacity Id.
Defaults to None which resolves to the capacity name of the workspace of the attached lakehouse
or if no lakehouse attached, resolves to the capacity name of the workspace of the notebook.
>
### Returns
> str; The capacity name.
### [resolve_dataset_id](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.resolve_dataset_id)
#### Obtains the ID of the semantic model.

```python
import sempy_labs as labs
labs.resolve_dataset_id(
    dataset='AdvWorks',
    workspace=None, # This parameter is optional
)
```

### Parameters
> **dataset** (str)
>
>> Required; The name of the semantic model.
>
> **workspace** (str, default=None)
>
>> Optional; The Fabric workspace name.
Defaults to None which resolves to the workspace of the attached lakehouse
or if no lakehouse attached, resolves to the workspace of the notebook.
>
### Returns
> UUID; The ID of the semantic model.
### [resolve_dataset_name](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.resolve_dataset_name)
#### Obtains the name of the semantic model.

```python
import sempy_labs as labs
labs.resolve_dataset_name(
    dataset_id='',
    workspace=None, # This parameter is optional
)
```

### Parameters
> **dataset_id** (UUID)
>
>> Required; The name of the semantic model.
>
> **workspace** (str, default=None)
>
>> Optional; The Fabric workspace name.
Defaults to None which resolves to the workspace of the attached lakehouse
or if no lakehouse attached, resolves to the workspace of the notebook.
>
### Returns
> str; The name of the semantic model.
### [resolve_item_type](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.resolve_item_type)
#### Obtains the item type for a given Fabric Item Id within a Fabric workspace.

```python
import sempy_labs as labs
labs.resolve_item_type(
    item_id='',
    workspace=None, # This parameter is optional
)
```

### Parameters
> **item_id** (UUID)
>
>> Required; The item/artifact Id.
>
> **workspace** (str, default=None)
>
>> Optional; The Fabric workspace name.
Defaults to None which resolves to the workspace of the attached lakehouse
or if no lakehouse attached, resolves to the workspace of the notebook.
>
### Returns
> str; The item type for the item Id.
### [resolve_lakehouse_id](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.resolve_lakehouse_id)
#### Obtains the ID of the Fabric lakehouse.

```python
import sempy_labs as labs
labs.resolve_lakehouse_id(
    lakehouse='',
    workspace=None, # This parameter is optional
)
```

### Parameters
> **lakehouse** (str)
>
>> Required; The name of the Fabric lakehouse.
>
> **workspace** (str, default=None)
>
>> Optional; The Fabric workspace name.
Defaults to None which resolves to the workspace of the attached lakehouse
or if no lakehouse attached, resolves to the workspace of the notebook.
>
### Returns
> UUID; The ID of the Fabric lakehouse.
### [resolve_lakehouse_name](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.resolve_lakehouse_name)
#### Obtains the name of the Fabric lakehouse.

```python
import sempy_labs as labs
labs.resolve_lakehouse_name(
    lakehouse_id=None, # This parameter is optional
    workspace=None, # This parameter is optional
)
```

### Parameters
> **lakehouse_id** (UUID, default=None)
>
>> Optional; The name of the Fabric lakehouse.
Defaults to None which resolves to the lakehouse attached to the notebook.
>
> **workspace** (str, default=None)
>
>> Optional; The Fabric workspace name.
Defaults to None which resolves to the workspace of the attached lakehouse
or if no lakehouse attached, resolves to the workspace of the notebook.
>
### Returns
> str; The name of the Fabric lakehouse.
### [resolve_report_id](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.resolve_report_id)
#### Obtains the ID of the Power BI report.

```python
import sempy_labs as labs
labs.resolve_report_id(
    report='',
    workspace=None, # This parameter is optional
)
```

### Parameters
> **report** (str)
>
>> Required; The name of the Power BI report.
>
> **workspace** (str, default=None)
>
>> Optional; The Fabric workspace name.
Defaults to None which resolves to the workspace of the attached lakehouse
or if no lakehouse attached, resolves to the workspace of the notebook.
>
### Returns
> UUID; The ID of the Power BI report.
### [resolve_report_name](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.resolve_report_name)
#### Obtains the name of the Power BI report.

```python
import sempy_labs as labs
labs.resolve_report_name(
    report_id='',
    workspace=None, # This parameter is optional
)
```

### Parameters
> **report_id** (UUID)
>
>> Required; The name of the Power BI report.
>
> **workspace** (str, default=None)
>
>> Optional; The Fabric workspace name.
Defaults to None which resolves to the workspace of the attached lakehouse
or if no lakehouse attached, resolves to the workspace of the notebook.
>
### Returns
> str; The name of the Power BI report.
### [resolve_workspace_capacity](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.resolve_workspace_capacity)
#### Obtains the capacity Id and capacity name for a given workspace.

```python
import sempy_labs as labs
labs.resolve_workspace_capacity(
    workspace=None, # This parameter is optional
)
```

### Parameters
> **workspace** (str, default=None)
>
>> Optional; The Fabric workspace name.
Defaults to None which resolves to the workspace of the attached lakehouse
or if no lakehouse attached, resolves to the workspace of the notebook.
>
### Returns
> Tuple[UUID, str]; capacity Id; capacity came.
### [restore_semantic_model](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.restore_semantic_model)
#### [Restores](https://learn.microsoft.com/power-bi/enterprise/service-premium-backup-restore-dataset) a semantic model based on a backup (.abf) file
within the ADLS Gen2 storage account connected to the workspace.
```python
import sempy_labs as labs
labs.restore_semantic_model(
    dataset='AdvWorks',
    file_path='',
    allow_overwrite=True, # This parameter is optional
    ignore_incompatibilities=True, # This parameter is optional
    force_restore=False, # This parameter is optional
    workspace=None, # This parameter is optional
)
```

### Parameters
> **dataset** (str)
>
>> Required; Name of the semantic model.
>
> **file_path** (str)
>
>> Required; The location in which to backup the semantic model. Must end in '.abf'.
Example 1: file_path = 'MyModel.abf'
Example 2: file_path = 'MyFolder/MyModel.abf'
>
> **allow_overwrite** (bool, default=True)
>
>> Optional; If True, overwrites backup files of the same name. If False, the file you are saving cannot have the same name as a file that already exists in the same location.
>
> **ignore_incompatibilities** (bool, default=True)
>
>> Optional; If True, ignores incompatibilities between Azure Analysis Services and Power BI Premium.
>
> **force_restore** (bool, default=False)
>
>> Optional; If True, restores the semantic model with the existing semantic model unloaded and offline.
>
> **workspace** (str, default=None)
>
>> Optional; The Fabric workspace name.
Defaults to None which resolves to the workspace of the attached lakehouse
or if no lakehouse attached, resolves to the workspace of the notebook.
>
### [run_model_bpa](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.run_model_bpa)
#### Displays an HTML visualization of the results of the Best Practice Analyzer scan for a semantic model.

```python
import sempy_labs as labs
labs.run_model_bpa(
    dataset='AdvWorks',
    rules=None, # This parameter is optional
    workspace=None, # This parameter is optional
    export=False, # This parameter is optional
    return_dataframe=False, # This parameter is optional
    extended=False, # This parameter is optional
    language=None, # This parameter is optional
)
```

### Parameters
> **dataset** (str)
>
>> Required; Name of the semantic model.
>
> **rules** (pandas.DataFrame, default=None)
>
>> Optional; A pandas dataframe containing rules to be evaluated.
>
> **workspace** (str, default=None)
>
>> Optional; The Fabric workspace name.
Defaults to None which resolves to the workspace of the attached lakehouse
or if no lakehouse attached, resolves to the workspace of the notebook.
>
> **export** (bool, default=False)
>
>> Optional; If True, exports the resulting dataframe to a delta table in the lakehouse attached to the notebook.
>
> **return_dataframe** (bool, default=False)
>
>> Optional; If True, returns a pandas dataframe instead of the visualization.
>
> **extended** (bool, default=False)
>
>> Optional; If True, runs the set_vertipaq_annotations function to collect Vertipaq Analyzer statistics to be used in the analysis of the semantic model.
>
> **language** (str, default=None)
>
>> Optional; Specifying a language code (i.e. 'it-IT' for Italian) will auto-translate the Category, Rule Name and Description into the specified language.
Defaults to None which resolves to English.
>
### Returns
> pandas.DataFrame; A pandas dataframe in HTML format showing semantic model objects which violated the best practice analyzer rules.
### [run_model_bpa_bulk](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.run_model_bpa_bulk)
#### Runs the semantic model Best Practice Analyzer across all semantic models in a workspace (or all accessible workspaces).
Saves (appends) the results to the 'modelbparesults' delta table in the lakehouse attached to the notebook.
Default semantic models are skipped in this analysis.
```python
import sempy_labs as labs
labs.run_model_bpa_bulk(
    rules=None, # This parameter is optional
    extended=False, # This parameter is optional
    language=None, # This parameter is optional
    workspace=None, # This parameter is optional
    skip_models=['ModelBPA', 'Fabric Capacity Metrics'], # This parameter is optional
)
```

### Parameters
> **dataset** (str)
>
>> Optional; Name of the semantic model.
>
> **rules** (pandas.DataFrame, default=None)
>
>> Optional; A pandas dataframe containing rules to be evaluated. Based on the format of the dataframe produced by the model_bpa_rules function.
>
> **extended** (bool, default=False)
>
>> Optional; If True, runs the set_vertipaq_annotations function to collect Vertipaq Analyzer statistics to be used in the analysis of the semantic model.
>
> **language** (str, default=None)
>
>> Optional; The language (code) in which the rules will appear. For example, specifying 'it-IT' will show the Rule Name, Category and Description in Italian.
Defaults to None which resolves to English.
>
> **workspace** (str | List[str], default=None)
>
>> Optional; The workspace or list of workspaces to scan.
Defaults to None which scans all accessible workspaces.
>
> **skip_models** (str | List[str], default=['ModelBPA', 'Fabric Capacity Metrics'])
>
>> Optional; The semantic models to always skip when running this analysis.
>
### [save_as_delta_table](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.save_as_delta_table)
#### Saves a pandas dataframe as a delta table in a Fabric lakehouse.

```python
import sempy_labs as labs
labs.save_as_delta_table(
    dataframe='',
    delta_table_name='',
    write_mode='',
    merge_schema=False, # This parameter is optional
    lakehouse=None, # This parameter is optional
    workspace=None, # This parameter is optional
)
```

### Parameters
> **dataframe** (pandas.DataFrame)
>
>> Required; The dataframe to be saved as a delta table.
>
> **delta_table_name** (str)
>
>> Required; The name of the delta table.
>
> **write_mode** (str)
>
>> Required; The write mode for the save operation. Options: 'append', 'overwrite'.
>
> **merge_schema** (bool, default=False)
>
>> Optional; Merges the schemas of the dataframe to the delta table.
>
> **lakehouse** (str, default=None)
>
>> Optional; The Fabric lakehouse used by the Direct Lake semantic model.
Defaults to None which resolves to the lakehouse attached to the notebook.
>
> **workspace** (str, default=None)
>
>> Optional; The Fabric workspace name.
Defaults to None which resolves to the workspace of the attached lakehouse
or if no lakehouse attached, resolves to the workspace of the notebook.
>
### Returns
> UUID; The ID of the Power BI report.
### [set_qso](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.set_qso)
#### Sets the query scale out settings for a semantic model.

```python
import sempy_labs as labs
labs.set_qso(
    dataset='AdvWorks',
    auto_sync=True, # This parameter is optional
    max_read_only_replicas='-1', # This parameter is optional
    workspace=None, # This parameter is optional
)
```

### Parameters
> **dataset** (str)
>
>> Required; Name of the semantic model.
>
> **auto_sync** (bool, default=True)
>
>> Optional; Whether the semantic model automatically syncs read-only replicas.
>
> **max_read_only_replicas** (int, default=-1)
>
>> Optional; To enable semantic model scale-out, set max_read_only_replicas to -1, or any non-0 value. A value of -1 allows Power BI to create as many read-only replicas as your Power BI capacity supports. You can also explicitly set the replica count to a value lower than that of the capacity maximum. Setting max_read_only_replicas to -1 is recommended.
>
> **workspace** (str, default=None)
>
>> Optional; The Fabric workspace name.
Defaults to None which resolves to the workspace of the attached lakehouse
or if no lakehouse attached, resolves to the workspace of the notebook.
>
### Returns
> pandas.DataFrame; A pandas dataframe showing the current query scale-out settings.
### [set_semantic_model_storage_format](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.set_semantic_model_storage_format)
#### Sets the semantic model storage format.

```python
import sempy_labs as labs
labs.set_semantic_model_storage_format(
    dataset='AdvWorks',
    storage_format='',
    workspace=None, # This parameter is optional
)
```

### Parameters
> **dataset** (str)
>
>> Required; Name of the semantic model.
>
> **storage_format** (str)
>
>> Required; The storage format for the semantic model. Valid options: 'Large', 'Small'.
>
> **workspace** (str, default=None)
>
>> Optional; The Fabric workspace name.
Defaults to None which resolves to the workspace of the attached lakehouse
or if no lakehouse attached, resolves to the workspace of the notebook.
>
### [set_workspace_default_storage_format](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.set_workspace_default_storage_format)
#### Sets the default storage format for semantic models within a workspace.

```python
import sempy_labs as labs
labs.set_workspace_default_storage_format(
    storage_format='',
    workspace=None, # This parameter is optional
)
```

### Parameters
> **storage_format** (str)
>
>> Required; The storage format for the semantic model. Valid options: 'Large', 'Small'.
>
> **workspace** (str, default=None)
>
>> Optional; The Fabric workspace name.
Defaults to None which resolves to the workspace of the attached lakehouse
or if no lakehouse attached, resolves to the workspace of the notebook.
>
### [translate_semantic_model](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.translate_semantic_model)
#### Translates names, descriptions, display folders for all objects in a semantic model.

```python
import sempy_labs as labs
labs.translate_semantic_model(
    dataset='AdvWorks',
    languages=['it-IT', 'zh-CN'],
    exclude_characters=None, # This parameter is optional
    workspace=None, # This parameter is optional
)
```

### Parameters
> **dataset** (str)
>
>> Required; Name of the semantic model.
>
> **languages** (str, List[str])
>
>> Required; The language code(s) in which to translate the semantic model.
>
> **exclude_characters** (str)
>
>> Optional; A string specifying characters which will be replaced by a space in the translation text when sent to the translation service.
>
> **workspace** (str, default=None)
>
>> Optional; The Fabric workspace name.
Defaults to None which resolves to the workspace of the attached lakehouse
or if no lakehouse attached, resolves to the workspace of the notebook.
>
### Returns
> pandas.DataFrame; Shows a pandas dataframe which displays all of the translations in the semantic model.
### [unassign_workspace_from_capacity](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.unassign_workspace_from_capacity)
#### Unassigns a workspace from its assigned capacity.

```python
import sempy_labs as labs
labs.unassign_workspace_from_capacity(
    workspace=None, # This parameter is optional
)
```

### Parameters
> **workspace** (str, default=None)
>
>> Optional; The name of the Fabric workspace.
Defaults to None which resolves to the workspace of the attached lakehouse
or if no lakehouse attached, resolves to the workspace of the notebook.
>
### [update_custom_pool](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.update_custom_pool)
#### Updates the properties of a [custom pool](https://learn.microsoft.com/fabric/data-engineering/create-custom-spark-pools) within a workspace.

```python
import sempy_labs as labs
labs.update_custom_pool(
    pool_name='',
    node_size=None, # This parameter is optional
    min_node_count=None, # This parameter is optional
    max_node_count=None, # This parameter is optional
    min_executors=None, # This parameter is optional
    max_executors=None, # This parameter is optional
    node_family=None, # This parameter is optional
    auto_scale_enabled=None, # This parameter is optional
    dynamic_executor_allocation_enabled=None, # This parameter is optional
    workspace=None, # This parameter is optional
)
```

### Parameters
> **pool_name** (str)
>
>> Required; The custom pool name.
>
> **node_size** (str, default=None)
>
>> Optional; The [node size](https://learn.microsoft.com/rest/api/fabric/spark/custom-pools/create-workspace-custom-pool?tabs=HTTP#nodesize).
Defaults to None which keeps the existing property setting.
>
> **min_node_count** (int, default=None)
>
>> Optional; The [minimum node count](https://learn.microsoft.com/rest/api/fabric/spark/custom-pools/create-workspace-custom-pool?tabs=HTTP#autoscaleproperties).
Defaults to None which keeps the existing property setting.
>
> **max_node_count** (int, default=None)
>
>> Optional; The [maximum node count](https://learn.microsoft.com/en-us/rest/api/fabric/spark/custom-pools/create-workspace-custom-pool?tabs=HTTP#autoscaleproperties).
Defaults to None which keeps the existing property setting.
>
> **min_executors** (int, default=None)
>
>> Optional; The [minimum executors](https://learn.microsoft.com/rest/api/fabric/spark/custom-pools/create-workspace-custom-pool?tabs=HTTP#dynamicexecutorallocationproperties).
Defaults to None which keeps the existing property setting.
>
> **max_executors** (int, default=None)
>
>> Optional; The [maximum executors](https://learn.microsoft.com/en-us/rest/api/fabric/spark/custom-pools/create-workspace-custom-pool?tabs=HTTP#dynamicexecutorallocationproperties).
Defaults to None which keeps the existing property setting.
>
> **node_family** (str, default=None)
>
>> Optional; The [node family](https://learn.microsoft.com/rest/api/fabric/spark/custom-pools/create-workspace-custom-pool?tabs=HTTP#nodefamily).
Defaults to None which keeps the existing property setting.
>
> **auto_scale_enabled** (bool, default=None)
>
>> Optional; The status of [auto scale](https://learn.microsoft.com/rest/api/fabric/spark/custom-pools/create-workspace-custom-pool?tabs=HTTP#autoscaleproperties).
Defaults to None which keeps the existing property setting.
>
> **dynamic_executor_allocation_enabled** (bool, default=None)
>
>> Optional; The status of the [dynamic executor allocation](https://learn.microsoft.com/en-us/rest/api/fabric/spark/custom-pools/create-workspace-custom-pool?tabs=HTTP#dynamicexecutorallocationproperties).
Defaults to None which keeps the existing property setting.
>
> **workspace** (str, default=None)
>
>> Optional; The name of the Fabric workspace.
Defaults to None which resolves to the workspace of the attached lakehouse
or if no lakehouse attached, resolves to the workspace of the notebook.
>
### [update_from_git](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.update_from_git)
#### Updates the workspace with commits pushed to the connected branch.

```python
import sempy_labs as labs
labs.update_from_git(
    remote_commit_hash='',
    conflict_resolution_policy='',
    workspace_head=None, # This parameter is optional
    allow_override=False, # This parameter is optional
    workspace=None, # This parameter is optional
)
```

### Parameters
> **workspace_head** (str)
>
>> Optional; Full SHA hash that the workspace is synced to. This value may be null only after Initialize Connection.
In other cases, the system will validate that the given value is aligned with the head known to the system.
>
> **remove_commit_hash** (str)
>
>> Optional; Remote full SHA commit hash.
>
> **confilict_resolution_policy** (str)
>
>> Optional; The [conflict resolution policy](https://learn.microsoft.com/rest/api/fabric/core/git/update-from-git?tabs=HTTP#conflictresolutionpolicy).
>
> **allow_override** (bool, default=False)
>
>> Optional; None
>
> **workspace** (str, default=None)
>
>> Optional; The Fabric workspace name.
Defaults to None which resolves to the workspace of the attached lakehouse
or if no lakehouse attached, resolves to the workspace of the notebook.
>
### [update_item](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.update_item)
#### Updates the name/description of a Fabric item.

```python
import sempy_labs as labs
labs.update_item(
    item_type='',
    current_name='',
    new_name='',
    description=None, # This parameter is optional
    workspace=None, # This parameter is optional
)
```

### Parameters
> **item_type** (str)
>
>> Required; Type of item to update.
>
> **current_name** (str)
>
>> Required; The current name of the item.
>
> **new_name** (str)
>
>> Required; The new name of the item.
>
> **description** (str, default=None)
>
>> Optional; A description of the item.
>
> **workspace** (str, default=None)
>
>> Optional; The Fabric workspace name.
Defaults to None which resolves to the workspace of the attached lakehouse
or if no lakehouse attached, resolves to the workspace of the notebook.
>
### [update_spark_settings](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.update_spark_settings)
#### Updates the spark settings for a workspace.

```python
import sempy_labs as labs
labs.update_spark_settings(
    automatic_log_enabled=None, # This parameter is optional
    high_concurrency_enabled=None, # This parameter is optional
    customize_compute_enabled=None, # This parameter is optional
    default_pool_name=None, # This parameter is optional
    max_node_count=None, # This parameter is optional
    max_executors=None, # This parameter is optional
    environment_name=None, # This parameter is optional
    runtime_version=None, # This parameter is optional
    workspace=None, # This parameter is optional
)
```

### Parameters
> **automatic_log_enabled** (bool, default=None)
>
>> Optional; The status of the [automatic log](https://learn.microsoft.com/rest/api/fabric/spark/workspace-settings/update-spark-settings?tabs=HTTP#automaticlogproperties).
Defaults to None which keeps the existing property setting.
>
> **high_concurrency_enabled** (bool, default=None)
>
>> Optional; The status of the [high concurrency](https://learn.microsoft.com/rest/api/fabric/spark/workspace-settings/update-spark-settings?tabs=HTTP#highconcurrencyproperties) for notebook interactive run.
Defaults to None which keeps the existing property setting.
>
> **customize_compute_enabled** (bool, default=None)
>
>> Optional; [Customize compute](https://learn.microsoft.com/rest/api/fabric/spark/workspace-settings/update-spark-settings?tabs=HTTP#poolproperties) configurations for items.
Defaults to None which keeps the existing property setting.
>
> **default_pool_name** (str, default=None)
>
>> Optional; [Default pool](https://learn.microsoft.com/rest/api/fabric/spark/workspace-settings/update-spark-settings?tabs=HTTP#poolproperties) for workspace.
Defaults to None which keeps the existing property setting.
>
> **max_node_count** (int, default=None)
>
>> Optional; The [maximum node count](https://learn.microsoft.com/en-us/rest/api/fabric/spark/workspace-settings/update-spark-settings?tabs=HTTP#starterpoolproperties).
Defaults to None which keeps the existing property setting.
>
> **max_executors** (int, default=None)
>
>> Optional; The [maximum executors](https://learn.microsoft.com/rest/api/fabric/spark/workspace-settings/update-spark-settings?tabs=HTTP#starterpoolproperties).
Defaults to None which keeps the existing property setting.
>
> **environment_name** (str, default=None)
>
>> Optional; The name of the [default environment](https://learn.microsoft.com/rest/api/fabric/spark/workspace-settings/update-spark-settings?tabs=HTTP#environmentproperties). Empty string indicated there is no workspace default environment
Defaults to None which keeps the existing property setting.
>
> **runtime_version** (str, default=None)
>
>> Optional; The [runtime version](https://learn.microsoft.com/rest/api/fabric/spark/workspace-settings/update-spark-settings?tabs=HTTP#environmentproperties).
Defaults to None which keeps the existing property setting.
>
> **workspace** (str, default=None)
>
>> Optional; The name of the Fabric workspace.
Defaults to None which resolves to the workspace of the attached lakehouse
or if no lakehouse attached, resolves to the workspace of the notebook.
>
### [update_workspace_user](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.update_workspace_user)
#### Updates a user's role within a workspace.

```python
import sempy_labs as labs
labs.update_workspace_user(
    email_address='hello@goodbye.com',
    role_name='',
    principal_type='User', # This parameter is optional
    workspace=None, # This parameter is optional
)
```

### Parameters
> **email_address** (str)
>
>> Required; The email address of the user.
>
> **role_name** (str)
>
>> Required; The [role](https://learn.microsoft.com/rest/api/power-bi/groups/add-group-user#groupuseraccessright) of the user within the workspace.
>
> **principal_type** (str, default='User')
>
>> Optional; The [principal type](https://learn.microsoft.com/rest/api/power-bi/groups/add-group-user#principaltype).
>
> **workspace** (str, default=None)
>
>> Optional; The name of the workspace.
Defaults to None which resolves to the workspace of the attached lakehouse
or if no lakehouse attached, resolves to the workspace of the notebook.
>
### [vertipaq_analyzer](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.vertipaq_analyzer)
#### Displays an HTML visualization of the Vertipaq Analyzer statistics from a semantic model.

```python
import sempy_labs as labs
labs.vertipaq_analyzer(
    dataset='AdvWorks',
    workspace=None, # This parameter is optional
    export=None, # This parameter is optional
    read_stats_from_data=False, # This parameter is optional
)
```

### Parameters
> **dataset** (str)
>
>> Required; Name of the semantic model.
>
> **workspace** (str, default=None)
>
>> Optional; The Fabric workspace name in which the semantic model exists.
Defaults to None which resolves to the workspace of the attached lakehouse
or if no lakehouse attached, resolves to the workspace of the notebook.
>
> **export** (str, default=None)
>
>> Optional; Specifying 'zip' will export the results to a zip file in your lakehouse (which can be imported using the import_vertipaq_analyzer function.
Specifying 'table' will export the results to delta tables (appended) in your lakehouse.
Default value: None.
>
> **read_stats_from_data** (bool, default=False)
>
>> Optional; Setting this parameter to true has the function get Column Cardinality and Missing Rows using DAX (Direct Lake semantic models achieve this using a Spark query to the lakehouse).
>
### [add_table_to_direct_lake_semantic_model](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.directlake.html#sempy_labs.directlake.add_table_to_direct_lake_semantic_model)
#### Adds a table and all of its columns to a Direct Lake semantic model, based on a Fabric lakehouse table.

```python
import sempy_labs as labs
import sempy_labs.directlake as directlake
directlake.add_table_to_direct_lake_semantic_model(
    dataset='AdvWorks',
    table_name='',
    lakehouse_table_name='',
    refresh=True, # This parameter is optional
    workspace=None, # This parameter is optional
)
```

### Parameters
> **dataset** (str)
>
>> Required; Name of the semantic model.
>
> **table_name** (str, List[str])
>
>> Required; Name of the table in the semantic model.
>
> **lakehouse_table_name** (str)
>
>> Required; The name of the Fabric lakehouse table.
>
> **refresh** (bool, default=True)
>
>> Optional; Refreshes the table after it is added to the semantic model.
>
> **workspace** (str, default=None)
>
>> Optional; The name of the Fabric workspace in which the semantic model resides.
Defaults to None which resolves to the workspace of the attached lakehouse
or if no lakehouse attached, resolves to the workspace of the notebook.
>
### [check_fallback_reason](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.directlake.html#sempy_labs.directlake.check_fallback_reason)
#### Shows the reason a table in a Direct Lake semantic model would fallback to DirectQuery.

```python
import sempy_labs as labs
import sempy_labs.directlake as directlake
directlake.check_fallback_reason(
    dataset='AdvWorks',
    workspace=None, # This parameter is optional
)
```

### Parameters
> **dataset** (str)
>
>> Required; Name of the semantic model.
>
> **workspace** (str, default=None)
>
>> Optional; The Fabric workspace name.
Defaults to None which resolves to the workspace of the attached lakehouse
or if no lakehouse attached, resolves to the workspace of the notebook.
>
### Returns
> pandas.DataFrame; The tables in the semantic model and their fallback reason.
### [direct_lake_schema_compare](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.directlake.html#sempy_labs.directlake.direct_lake_schema_compare)
#### Checks that all the tables in a Direct Lake semantic model map to tables in their corresponding lakehouse and that the columns in each table exist.

```python
import sempy_labs as labs
import sempy_labs.directlake as directlake
directlake.direct_lake_schema_compare(
    dataset='AdvWorks',
    workspace=None, # This parameter is optional
)
```

### Parameters
> **dataset** (str)
>
>> Required; Name of the semantic model.
>
> **workspace** (str, default=None)
>
>> Optional; The Fabric workspace name.
Defaults to None which resolves to the workspace of the attached lakehouse
or if no lakehouse attached, resolves to the workspace of the notebook.
>
### [direct_lake_schema_sync](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.directlake.html#sempy_labs.directlake.direct_lake_schema_sync)
#### Shows/adds columns which exist in the lakehouse but do not exist in the semantic model (only for tables in the semantic model).

```python
import sempy_labs as labs
import sempy_labs.directlake as directlake
directlake.direct_lake_schema_sync(
    dataset='AdvWorks',
    workspace=None, # This parameter is optional
    add_to_model=False, # This parameter is optional
)
```

### Parameters
> **dataset** (str)
>
>> Required; Name of the semantic model.
>
> **workspace** (str, default=None)
>
>> Optional; The Fabric workspace name.
Defaults to None which resolves to the workspace of the attached lakehouse
or if no lakehouse attached, resolves to the workspace of the notebook.
>
> **add_to_model** (bool, default=False)
>
>> Optional; If set to True, columns which exist in the lakehouse but do not exist in the semantic model are added to the semantic model. No new tables are added.
>
### [generate_direct_lake_semantic_model](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.directlake.html#sempy_labs.directlake.generate_direct_lake_semantic_model)
#### Dynamically generates a Direct Lake semantic model based on tables in a Fabric lakehouse.

```python
import sempy_labs as labs
import sempy_labs.directlake as directlake
directlake.generate_direct_lake_semantic_model(
    dataset='AdvWorks',
    lakehouse_tables='',
    workspace=None, # This parameter is optional
    lakehouse=None, # This parameter is optional
    lakehouse_workspace=None, # This parameter is optional
    overwrite=False, # This parameter is optional
    refresh=True, # This parameter is optional
)
```

### Parameters
> **dataset** (str)
>
>> Required; Name of the semantic model to be created.
>
> **lakehouse_tables** (str | List[str])
>
>> Required; The table(s) within the Fabric lakehouse to add to the semantic model. All columns from these tables will be added to the semantic model.
>
> **workspace** (str, default=None)
>
>> Optional; The Fabric workspace name in which the semantic model will reside.
Defaults to None which resolves to the workspace of the attached lakehouse
or if no lakehouse attached, resolves to the workspace of the notebook.
>
> **lakehouse** (str, default=None)
>
>> Optional; The lakehouse which stores the delta tables which will feed the Direct Lake semantic model.
Defaults to None which resolves to the attached lakehouse.
>
> **lakehouse_workspace** (str, default=None)
>
>> Optional; The Fabric workspace in which the lakehouse resides.
Defaults to None which resolves to the workspace of the attached lakehouse
or if no lakehouse attached, resolves to the workspace of the notebook.
>
> **overwrite** (bool, default=False)
>
>> Optional; If set to True, overwrites the existing semantic model if it already exists.
>
> **refresh** (bool, default=True)
>
>> Optional; If True, refreshes the newly created semantic model after it is created.
>
### [get_direct_lake_guardrails](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.directlake.html#sempy_labs.directlake.get_direct_lake_guardrails)
#### Shows the guardrails for when Direct Lake semantic models will fallback to Direct Query
based on Microsoft's [online documentation](https://learn.microsoft.com/power-bi/enterprise/directlake-overview).
```python
import sempy_labs as labs
import sempy_labs.directlake as directlake
directlake.get_direct_lake_guardrails()
```

### Returns
> pandas.DataFrame; A table showing the Direct Lake guardrails by SKU.
### [get_direct_lake_lakehouse](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.directlake.html#sempy_labs.directlake.get_direct_lake_lakehouse)
#### Identifies the lakehouse used by a Direct Lake semantic model.

```python
import sempy_labs as labs
import sempy_labs.directlake as directlake
directlake.get_direct_lake_lakehouse(
    dataset='AdvWorks',
    workspace=None, # This parameter is optional
    lakehouse=None, # This parameter is optional
    lakehouse_workspace=None, # This parameter is optional
)
```

### Parameters
> **dataset** (str)
>
>> Required; Name of the semantic model.
>
> **workspace** (str, default=None)
>
>> Optional; The Fabric workspace name.
Defaults to None which resolves to the workspace of the attached lakehouse
or if no lakehouse attached, resolves to the workspace of the notebook.
>
> **lakehouse** (str, default=None)
>
>> Optional; The Fabric lakehouse used by the Direct Lake semantic model.
Defaults to None which resolves to the lakehouse attached to the notebook.
>
> **lakehouse_workspace** (str, default=None)
>
>> Optional; The Fabric workspace used by the lakehouse.
Defaults to None which resolves to the workspace of the attached lakehouse
or if no lakehouse attached, resolves to the workspace of the notebook.
>
### Returns
> str, uuid.UUID; The lakehouse name and lakehouse ID.
### [get_direct_lake_source](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.directlake.html#sempy_labs.directlake.get_direct_lake_source)
#### Obtains the source information for a direct lake semantic model.

```python
import sempy_labs as labs
import sempy_labs.directlake as directlake
directlake.get_direct_lake_source(
    dataset='AdvWorks',
    workspace=None, # This parameter is optional
)
```

### Parameters
> **dataset** (str)
>
>> Required; The name of the semantic model.
>
> **workspace** (str, default=None)
>
>> Optional; The Fabric workspace name.
Defaults to None which resolves to the workspace of the attached lakehouse
or if no lakehouse attached, resolves to the workspace of the notebook.
>
### Returns
> Tuple[str, str, UUID, UUID]; If the source of the direct lake semantic model is a lakehouse this will return: 'Lakehouse', Lakehouse Name, SQL Endpoint Id, Workspace Id
If the source of the direct lake semantic model is a warehouse this will return: 'Warehouse', Warehouse Name, Warehouse Id, Workspace Id
If the semantic model is not a Direct Lake semantic model, it will return None, None, None.
### [get_directlake_guardrails_for_sku](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.directlake.html#sempy_labs.directlake.get_directlake_guardrails_for_sku)
#### Shows the guardrails for Direct Lake based on the SKU used by your workspace's capacity.
* Use the result of the 'get_sku_size' function as an input for this function's sku_size parameter.*
```python
import sempy_labs as labs
import sempy_labs.directlake as directlake
directlake.get_directlake_guardrails_for_sku(
    sku_size='',
)
```

### Parameters
> **sku_size** (str)
>
>> Required; Sku size of a workspace/capacity
>
### Returns
> pandas.DataFrame; A table showing the Direct Lake guardrails for the given SKU.
### [get_shared_expression](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.directlake.html#sempy_labs.directlake.get_shared_expression)
#### Dynamically generates the M expression used by a Direct Lake model for a given lakehouse.

```python
import sempy_labs as labs
import sempy_labs.directlake as directlake
directlake.get_shared_expression(
    lakehouse=None, # This parameter is optional
    workspace=None, # This parameter is optional
)
```

### Parameters
> **lakehouse** (str, default=None)
>
>> Optional; The Fabric lakehouse used by the Direct Lake semantic model.
Defaults to None which resolves to the lakehouse attached to the notebook.
>
> **workspace** (str, default=None)
>
>> Optional; The Fabric workspace used by the lakehouse.
Defaults to None which resolves to the workspace of the attached lakehouse
or if no lakehouse attached, resolves to the workspace of the notebook.
>
### Returns
> str; Shows the expression which can be used to connect a Direct Lake semantic model to its SQL Endpoint.
### [get_sku_size](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.directlake.html#sempy_labs.directlake.get_sku_size)
#### Shows the SKU size for a workspace.

```python
import sempy_labs as labs
import sempy_labs.directlake as directlake
directlake.get_sku_size(
    workspace=None, # This parameter is optional
)
```

### Parameters
> **workspace** (str, default=None)
>
>> Optional; The Fabric workspace name.
Defaults to None which resolves to the workspace of the attached lakehouse
or if no lakehouse attached, resolves to the workspace of the notebook.
>
### Returns
> str; The SKU size for a workspace.
### [list_direct_lake_model_calc_tables](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.directlake.html#sempy_labs.directlake.list_direct_lake_model_calc_tables)
#### Shows the calculated tables and their respective DAX expression for a Direct Lake model (which has been migrated from import/DirectQuery).

```python
import sempy_labs as labs
import sempy_labs.directlake as directlake
directlake.list_direct_lake_model_calc_tables(
    dataset='AdvWorks',
    workspace=None, # This parameter is optional
)
```

### Parameters
> **dataset** (str)
>
>> Required; Name of the semantic model.
>
> **workspace** (str, default=None)
>
>> Optional; The Fabric workspace name.
Defaults to None which resolves to the workspace of the attached lakehouse
or if no lakehouse attached, resolves to the workspace of the notebook.
>
### Returns
> pandas.DataFrame; A pandas dataframe showing the calculated tables which were migrated to Direct Lake and whose DAX expressions are stored as model annotations.
### [show_unsupported_direct_lake_objects](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.directlake.html#sempy_labs.directlake.show_unsupported_direct_lake_objects)
#### Returns a list of a semantic model's objects which are not supported by Direct Lake based on
[official documentation](https://learn.microsoft.com/power-bi/enterprise/directlake-overview#known-issues-and-limitations).
```python
import sempy_labs as labs
import sempy_labs.directlake as directlake
directlake.show_unsupported_direct_lake_objects(
    dataset='AdvWorks',
    workspace=None, # This parameter is optional
)
```

### Parameters
> **dataset** (str)
>
>> Required; Name of the semantic model.
>
> **workspace** (str, default=None)
>
>> Optional; The Fabric workspace name.
Defaults to None which resolves to the workspace of the attached lakehouse
or if no lakehouse attached, resolves to the workspace of the notebook.
>
### Returns
> pandas.DataFrame, pandas.DataFrame, pandas.DataFrame; 3 pandas dataframes showing objects in a semantic model which are not supported by Direct Lake.
### [update_direct_lake_model_lakehouse_connection](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.directlake.html#sempy_labs.directlake.update_direct_lake_model_lakehouse_connection)
#### Remaps a Direct Lake semantic model's SQL Endpoint connection to a new lakehouse.

```python
import sempy_labs as labs
import sempy_labs.directlake as directlake
directlake.update_direct_lake_model_lakehouse_connection(
    dataset='AdvWorks',
    workspace=None, # This parameter is optional
    lakehouse=None, # This parameter is optional
    lakehouse_workspace=None, # This parameter is optional
)
```

### Parameters
> **dataset** (str)
>
>> Required; Name of the semantic model.
>
> **workspace** (str, default=None)
>
>> Optional; The Fabric workspace name in which the semantic model exists.
Defaults to None which resolves to the workspace of the attached lakehouse
or if no lakehouse attached, resolves to the workspace of the notebook.
>
> **lakehouse** (str, default=None)
>
>> Optional; The Fabric lakehouse used by the Direct Lake semantic model.
Defaults to None which resolves to the lakehouse attached to the notebook.
>
> **lakehouse_workspace** (str, default=None)
>
>> Optional; The Fabric workspace used by the lakehouse.
Defaults to None which resolves to the workspace of the attached lakehouse
or if no lakehouse attached, resolves to the workspace of the notebook.
>
### [update_direct_lake_partition_entity](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.directlake.html#sempy_labs.directlake.update_direct_lake_partition_entity)
#### Remaps a table (or tables) in a Direct Lake semantic model to a table in a lakehouse.

```python
import sempy_labs as labs
import sempy_labs.directlake as directlake
directlake.update_direct_lake_partition_entity(
    dataset='AdvWorks',
    table_name='',
    entity_name='',
    workspace=None, # This parameter is optional
)
```

### Parameters
> **dataset** (str)
>
>> Required; Name of the semantic model.
>
> **table_name** (str, List[str])
>
>> Required; Name of the table(s) in the semantic model.
>
> **entity_name** (str, List[str])
>
>> Required; Name of the lakehouse table to be mapped to the semantic model table.
>
> **workspace** (str, default=None)
>
>> Optional; The Fabric workspace name in which the semantic model exists.
Defaults to None which resolves to the workspace of the attached lakehouse
or if no lakehouse attached, resolves to the workspace of the notebook.
>
### [warm_direct_lake_cache_isresident](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.directlake.html#sempy_labs.directlake.warm_direct_lake_cache_isresident)
#### Performs a refresh on the semantic model and puts the columns which were in memory prior to the refresh back into memory.

```python
import sempy_labs as labs
import sempy_labs.directlake as directlake
directlake.warm_direct_lake_cache_isresident(
    dataset='AdvWorks',
    workspace=None, # This parameter is optional
)
```

### Parameters
> **dataset** (str)
>
>> Required; Name of the semantic model.
>
> **workspace** (str, default=None)
>
>> Optional; The Fabric workspace name.
Defaults to None which resolves to the workspace of the attached lakehouse
or if no lakehouse attached, resolves to the workspace of the notebook.
>
### Returns
> pandas.DataFrame; Returns a pandas dataframe showing the columns that have been put into memory.
### [warm_direct_lake_cache_perspective](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.directlake.html#sempy_labs.directlake.warm_direct_lake_cache_perspective)
#### Warms the cache of a Direct Lake semantic model by running a simple DAX query against the columns in a perspective.

```python
import sempy_labs as labs
import sempy_labs.directlake as directlake
directlake.warm_direct_lake_cache_perspective(
    dataset='AdvWorks',
    perspective='',
    add_dependencies=False, # This parameter is optional
    workspace=None, # This parameter is optional
)
```

### Parameters
> **dataset** (str)
>
>> Required; Name of the semantic model.
>
> **perspective** (str)
>
>> Required; Name of the perspective which contains objects to be used for warming the cache.
>
> **add_dependencies** (bool, default=False)
>
>> Optional; Includes object dependencies in the cache warming process.
>
> **workspace** (str, default=None)
>
>> Optional; The Fabric workspace name.
Defaults to None which resolves to the workspace of the attached lakehouse
or if no lakehouse attached, resolves to the workspace of the notebook.
>
### Returns
> pandas.DataFrame; Returns a pandas dataframe showing the columns that have been put into memory.
### [create_shortcut_onelake](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.lakehouse.html#sempy_labs.lakehouse.create_shortcut_onelake)
#### Creates a [shortcut](https://learn.microsoft.com/fabric/onelake/onelake-shortcuts) to a delta table in OneLake.

```python
import sempy_labs as labs
import sempy_labs.lakehouse as lake
lake.create_shortcut_onelake(
    table_name='',
    source_lakehouse='',
    source_workspace='',
    destination_lakehouse='',
    destination_workspace=None, # This parameter is optional
    shortcut_name=None, # This parameter is optional
)
```

### Parameters
> **table_name** (str)
>
>> Required; The table name for which a shortcut will be created.
>
> **source_lakehouse** (str)
>
>> Required; The Fabric lakehouse in which the table resides.
>
> **source_workspace** (str)
>
>> Required; The name of the Fabric workspace in which the source lakehouse exists.
>
> **destination_lakehouse** (str)
>
>> Required; The Fabric lakehouse in which the shortcut will be created.
>
> **destination_workspace** (str, default=None)
>
>> Optional; The name of the Fabric workspace in which the shortcut will be created.
Defaults to None which resolves to the workspace of the attached lakehouse
or if no lakehouse attached, resolves to the workspace of the notebook.
>
> **shortcut_name** (str, default=None)
>
>> Optional; The name of the shortcut 'table' to be created. This defaults to the 'table_name' parameter value.
>
### [delete_shortcut](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.lakehouse.html#sempy_labs.lakehouse.delete_shortcut)
#### Deletes a shortcut.

```python
import sempy_labs as labs
import sempy_labs.lakehouse as lake
lake.delete_shortcut(
    shortcut_name='',
    lakehouse=None, # This parameter is optional
    workspace=None, # This parameter is optional
)
```

### Parameters
> **shortcut_name** (str)
>
>> Required; The name of the shortcut.
>
> **lakehouse** (str, default=None)
>
>> Optional; The Fabric lakehouse name in which the shortcut resides.
Defaults to None which resolves to the lakehouse attached to the notebook.
>
> **workspace** (str, default=None)
>
>> Optional; The name of the Fabric workspace in which lakehouse resides.
Defaults to None which resolves to the workspace of the attached lakehouse
or if no lakehouse attached, resolves to the workspace of the notebook.
>
### [get_lakehouse_columns](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.lakehouse.html#sempy_labs.lakehouse.get_lakehouse_columns)
#### Shows the tables and columns of a lakehouse and their respective properties.

```python
import sempy_labs as labs
import sempy_labs.lakehouse as lake
lake.get_lakehouse_columns(
    lakehouse=None, # This parameter is optional
    workspace=None, # This parameter is optional
)
```

### Parameters
> **lakehouse** (str, default=None)
>
>> Optional; The Fabric lakehouse.
Defaults to None which resolves to the lakehouse attached to the notebook.
>
> **lakehouse_workspace** (str, default=None)
>
>> Optional; The Fabric workspace used by the lakehouse.
Defaults to None which resolves to the workspace of the attached lakehouse
or if no lakehouse attached, resolves to the workspace of the notebook.
>
### Returns
> pandas.DataFrame; Shows the tables/columns within a lakehouse and their properties.
### [get_lakehouse_tables](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.lakehouse.html#sempy_labs.lakehouse.get_lakehouse_tables)
#### Shows the tables of a lakehouse and their respective properties. Option to include additional properties relevant to Direct Lake guardrails.

```python
import sempy_labs as labs
import sempy_labs.lakehouse as lake
lake.get_lakehouse_tables(
    lakehouse=None, # This parameter is optional
    workspace=None, # This parameter is optional
    extended=False, # This parameter is optional
    count_rows=False, # This parameter is optional
    export=False, # This parameter is optional
)
```

### Parameters
> **lakehouse** (str, default=None)
>
>> Optional; The Fabric lakehouse.
Defaults to None which resolves to the lakehouse attached to the notebook.
>
> **lakehouse_workspace** (str, default=None)
>
>> Optional; The Fabric workspace used by the lakehouse.
Defaults to None which resolves to the workspace of the attached lakehouse
or if no lakehouse attached, resolves to the workspace of the notebook.
>
> **extended** (bool, default=False)
>
>> Optional; Obtains additional columns relevant to the size of each table.
>
> **count_rows** (bool, default=False)
>
>> Optional; Obtains a row count for each lakehouse table.
>
> **export** (bool, default=False)
>
>> Optional; Exports the resulting dataframe to a delta table in the lakehouse.
>
### Returns
> pandas.DataFrame; Shows the tables/columns within a lakehouse and their properties.
### [lakehouse_attached](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.lakehouse.html#sempy_labs.lakehouse.lakehouse_attached)
#### Identifies if a lakehouse is attached to the notebook.

```python
import sempy_labs as labs
import sempy_labs.lakehouse as lake
lake.lakehouse_attached()
```

### Returns
> bool; Returns True if a lakehouse is attached to the notebook.
### [optimize_lakehouse_tables](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.lakehouse.html#sempy_labs.lakehouse.optimize_lakehouse_tables)
#### Runs the [OPTIMIZE](https://docs.delta.io/latest/optimizations-oss.html) function over the specified lakehouse tables.

```python
import sempy_labs as labs
import sempy_labs.lakehouse as lake
lake.optimize_lakehouse_tables(
    tables=None, # This parameter is optional
    lakehouse=None, # This parameter is optional
    workspace=None, # This parameter is optional
)
```

### Parameters
> **tables** (str | List[str], default=None)
>
>> Optional; The table(s) to optimize.
Defaults to None which resovles to optimizing all tables within the lakehouse.
>
> **lakehouse** (str, default=None)
>
>> Optional; The Fabric lakehouse.
Defaults to None which resolves to the lakehouse attached to the notebook.
>
> **workspace** (str, default=None)
>
>> Optional; The Fabric workspace used by the lakehouse.
Defaults to None which resolves to the workspace of the attached lakehouse
or if no lakehouse attached, resolves to the workspace of the notebook.
>
### [vacuum_lakehouse_tables](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.lakehouse.html#sempy_labs.lakehouse.vacuum_lakehouse_tables)
#### Runs the [VACUUM](https://docs.delta.io/latest/delta-utility.html#remove-files-no-longer-referenced-by-a-delta-table) function over the specified lakehouse tables.

```python
import sempy_labs as labs
import sempy_labs.lakehouse as lake
lake.vacuum_lakehouse_tables(
    tables=None, # This parameter is optional
    lakehouse=None, # This parameter is optional
    workspace=None, # This parameter is optional
    retain_n_hours=None, # This parameter is optional
)
```

### Parameters
> **tables** (str | List[str] | None)
>
>> Optional; The table(s) to vacuum. If no tables are specified, all tables in the lakehouse will be optimized.
>
> **lakehouse** (str, default=None)
>
>> Optional; The Fabric lakehouse.
Defaults to None which resolves to the lakehouse attached to the notebook.
>
> **workspace** (str, default=None)
>
>> Optional; The Fabric workspace used by the lakehouse.
Defaults to None which resolves to the workspace of the attached lakehouse
or if no lakehouse attached, resolves to the workspace of the notebook.
>
> **retain_n_hours** (int, default=None)
>
>> Optional; The number of hours to retain historical versions of Delta table files.
Files older than this retention period will be deleted during the vacuum operation.
If not specified, the default retention period configured for the Delta table will be used.
The default retention period is 168 hours (7 days) unless manually configured via table properties.
>
### [create_pqt_file](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.migration.html#sempy_labs.migration.create_pqt_file)
#### Dynamically generates a [Power Query Template](https://learn.microsoft.com/power-query/power-query-template) file based on the semantic model. The .pqt file is
saved within the Files section of your lakehouse.

Dataflows Gen2 has a `limit of 50 tables <https://learn.microsoft.com/power-query/power-query-online-limits>`_. If there are more than 50 tables, this will save multiple Power Query Template
files (with each file having a max of 50 tables).
```python
import sempy_labs as labs
import sempy_labs.migration as migration
migration.create_pqt_file(
    dataset='AdvWorks',
    workspace=None, # This parameter is optional
    file_name='PowerQueryTemplate', # This parameter is optional
)
```

### Parameters
> **dataset** (str)
>
>> Required; Name of the semantic model.
>
> **workspace** (str, default=None)
>
>> Optional; The Fabric workspace name.
Defaults to None which resolves to the workspace of the attached lakehouse
or if no lakehouse attached, resolves to the workspace of the notebook.
>
> **file_name** (str, default='PowerQueryTemplate')
>
>> Optional; The name of the Power Query Template file to be generated.
>
### [migrate_calc_tables_to_lakehouse](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.migration.html#sempy_labs.migration.migrate_calc_tables_to_lakehouse)
#### Creates delta tables in your lakehouse based on the DAX expression of a calculated table in an import/DirectQuery semantic model.
The DAX expression encapsulating the calculated table logic is stored in the new Direct Lake semantic model as model annotations.
```python
import sempy_labs as labs
import sempy_labs.migration as migration
migration.migrate_calc_tables_to_lakehouse(
    dataset='AdvWorks',
    new_dataset='',
    workspace=None, # This parameter is optional
    new_dataset_workspace=None, # This parameter is optional
    lakehouse=None, # This parameter is optional
    lakehouse_workspace=None, # This parameter is optional
)
```

### Parameters
> **dataset** (str)
>
>> Required; Name of the import/DirectQuery semantic model.
>
> **new_dataset** (str)
>
>> Required; Name of the Direct Lake semantic model.
>
> **workspace** (str, default=None)
>
>> Optional; The Fabric workspace name in which the import/DirectQuery semantic model exists.
Defaults to None which resolves to the workspace of the attached lakehouse
or if no lakehouse attached, resolves to the workspace of the notebook.
>
> **new_dataset_workspace** (str)
>
>> Optional; The Fabric workspace name in which the Direct Lake semantic model will be created.
Defaults to None which resolves to the workspace of the attached lakehouse
or if no lakehouse attached, resolves to the workspace of the notebook.
>
> **lakehouse** (str, default=None)
>
>> Optional; The Fabric lakehouse used by the Direct Lake semantic model.
Defaults to None which resolves to the lakehouse attached to the notebook.
>
> **lakehouse_workspace** (str, default=None)
>
>> Optional; The Fabric workspace used by the lakehouse.
Defaults to None which resolves to the workspace of the attached lakehouse
or if no lakehouse attached, resolves to the workspace of the notebook.
>
### [migrate_calc_tables_to_semantic_model](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.migration.html#sempy_labs.migration.migrate_calc_tables_to_semantic_model)
#### Creates new tables in the Direct Lake semantic model based on the lakehouse tables created using the 'migrate_calc_tables_to_lakehouse' function.

```python
import sempy_labs as labs
import sempy_labs.migration as migration
migration.migrate_calc_tables_to_semantic_model(
    dataset='AdvWorks',
    new_dataset='',
    workspace=None, # This parameter is optional
    new_dataset_workspace=None, # This parameter is optional
    lakehouse=None, # This parameter is optional
    lakehouse_workspace=None, # This parameter is optional
)
```

### Parameters
> **dataset** (str)
>
>> Required; Name of the import/DirectQuery semantic model.
>
> **new_dataset** (str)
>
>> Required; Name of the Direct Lake semantic model.
>
> **workspace** (str, default=None)
>
>> Optional; The Fabric workspace name in which the import/DirectQuery semantic model exists.
Defaults to None which resolves to the workspace of the attached lakehouse
or if no lakehouse attached, resolves to the workspace of the notebook.
>
> **new_dataset_workspace** (str)
>
>> Optional; The Fabric workspace name in which the Direct Lake semantic model will be created.
Defaults to None which resolves to the workspace of the attached lakehouse
or if no lakehouse attached, resolves to the workspace of the notebook.
>
> **lakehouse** (str, default=None)
>
>> Optional; The Fabric lakehouse used by the Direct Lake semantic model.
Defaults to None which resolves to the lakehouse attached to the notebook.
>
> **lakehouse_workspace** (str, default=None)
>
>> Optional; The Fabric workspace used by the lakehouse.
Defaults to None which resolves to the workspace of the attached lakehouse
or if no lakehouse attached, resolves to the workspace of the notebook.
>
### [migrate_field_parameters](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.migration.html#sempy_labs.migration.migrate_field_parameters)
#### Migrates field parameters from one semantic model to another.

```python
import sempy_labs as labs
import sempy_labs.migration as migration
migration.migrate_field_parameters(
    dataset='AdvWorks',
    new_dataset='',
    workspace=None, # This parameter is optional
    new_dataset_workspace=None, # This parameter is optional
)
```

### Parameters
> **dataset** (str)
>
>> Required; Name of the import/DirectQuery semantic model.
>
> **new_dataset** (str)
>
>> Required; Name of the Direct Lake semantic model.
>
> **workspace** (str, default=None)
>
>> Optional; The Fabric workspace name in which the import/DirectQuery semantic model exists.
Defaults to None which resolves to the workspace of the attached lakehouse
or if no lakehouse attached, resolves to the workspace of the notebook.
>
> **new_dataset_workspace** (str)
>
>> Optional; The Fabric workspace name in which the Direct Lake semantic model will be created.
Defaults to None which resolves to the workspace of the attached lakehouse
or if no lakehouse attached, resolves to the workspace of the notebook.
>
### [migrate_model_objects_to_semantic_model](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.migration.html#sempy_labs.migration.migrate_model_objects_to_semantic_model)
#### Adds the rest of the model objects (besides tables/columns) and their properties to a Direct Lake semantic model based on an import/DirectQuery semantic model.

```python
import sempy_labs as labs
import sempy_labs.migration as migration
migration.migrate_model_objects_to_semantic_model(
    dataset='AdvWorks',
    new_dataset='',
    workspace=None, # This parameter is optional
    new_dataset_workspace=None, # This parameter is optional
)
```

### Parameters
> **dataset** (str)
>
>> Required; Name of the import/DirectQuery semantic model.
>
> **new_dataset** (str)
>
>> Required; Name of the Direct Lake semantic model.
>
> **workspace** (str, default=None)
>
>> Optional; The Fabric workspace name in which the import/DirectQuery semantic model exists.
Defaults to None which resolves to the workspace of the attached lakehouse
or if no lakehouse attached, resolves to the workspace of the notebook.
>
> **new_dataset_workspace** (str)
>
>> Optional; The Fabric workspace name in which the Direct Lake semantic model will be created.
Defaults to None which resolves to the workspace of the attached lakehouse
or if no lakehouse attached, resolves to the workspace of the notebook.
>
### [migrate_tables_columns_to_semantic_model](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.migration.html#sempy_labs.migration.migrate_tables_columns_to_semantic_model)
#### Adds tables/columns to the new Direct Lake semantic model based on an import/DirectQuery semantic model.

```python
import sempy_labs as labs
import sempy_labs.migration as migration
migration.migrate_tables_columns_to_semantic_model(
    dataset='AdvWorks',
    new_dataset='',
    workspace=None, # This parameter is optional
    new_dataset_workspace=None, # This parameter is optional
    lakehouse=None, # This parameter is optional
    lakehouse_workspace=None, # This parameter is optional
)
```

### Parameters
> **dataset** (str)
>
>> Required; Name of the import/DirectQuery semantic model.
>
> **new_dataset** (str)
>
>> Required; Name of the Direct Lake semantic model.
>
> **workspace** (str, default=None)
>
>> Optional; The Fabric workspace name in which the import/DirectQuery semantic model exists.
Defaults to None which resolves to the workspace of the attached lakehouse
or if no lakehouse attached, resolves to the workspace of the notebook.
>
> **new_dataset_workspace** (str)
>
>> Optional; The Fabric workspace name in which the Direct Lake semantic model will be created.
Defaults to None which resolves to the workspace of the attached lakehouse
or if no lakehouse attached, resolves to the workspace of the notebook.
>
> **lakehouse** (str, default=None)
>
>> Optional; The Fabric lakehouse used by the Direct Lake semantic model.
Defaults to None which resolves to the lakehouse attached to the notebook.
>
> **lakehouse_workspace** (str, default=None)
>
>> Optional; The Fabric workspace used by the lakehouse.
Defaults to None which resolves to the workspace of the attached lakehouse
or if no lakehouse attached, resolves to the workspace of the notebook.
>
### [migration_validation](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.migration.html#sempy_labs.migration.migration_validation)
#### Shows the objects in the original semantic model and whether then were migrated successfully or not.

```python
import sempy_labs as labs
import sempy_labs.migration as migration
migration.migration_validation(
    dataset='AdvWorks',
    new_dataset='',
    workspace=None, # This parameter is optional
    new_dataset_workspace=None, # This parameter is optional
)
```

### Parameters
> **dataset** (str)
>
>> Required; Name of the import/DirectQuery semantic model.
>
> **new_dataset** (str)
>
>> Required; Name of the Direct Lake semantic model.
>
> **workspace** (str, default=None)
>
>> Optional; The Fabric workspace name in which the import/DirectQuery semantic model exists.
Defaults to None which resolves to the workspace of the attached lakehouse
or if no lakehouse attached, resolves to the workspace of the notebook.
>
> **new_dataset_workspace** (str)
>
>> Optional; The Fabric workspace name in which the Direct Lake semantic model will be created.
Defaults to None which resolves to the workspace of the attached lakehouse
or if no lakehouse attached, resolves to the workspace of the notebook.
>
### Returns
> pandas.DataFrame; A pandas dataframe showing a list of objects and whether they were successfully migrated. Also shows the % of objects which were migrated successfully.
### [refresh_calc_tables](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.migration.html#sempy_labs.migration.refresh_calc_tables)
#### Recreates the delta tables in the lakehouse based on the DAX expressions stored as model annotations in the Direct Lake semantic model.

```python
import sempy_labs as labs
import sempy_labs.migration as migration
migration.refresh_calc_tables(
    dataset='AdvWorks',
    workspace=None, # This parameter is optional
)
```

### Parameters
> **dataset** (str)
>
>> Required; Name of the semantic model.
>
> **workspace** (str, default=None)
>
>> Optional; The Fabric workspace name.
Defaults to None which resolves to the workspace of the attached lakehouse
or if no lakehouse attached, resolves to the workspace of the notebook.
>
### [clone_report](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.report.html#sempy_labs.report.clone_report)
#### Clones a Power BI report.

```python
import sempy_labs as labs
import sempy_labs.report as rep
rep.clone_report(
    report='',
    cloned_report='',
    workspace=None, # This parameter is optional
    target_workspace=None, # This parameter is optional
    target_dataset=None, # This parameter is optional
    target_dataset_workspace=None, # This parameter is optional
)
```

### Parameters
> **report** (str)
>
>> Required; Name of the Power BI report.
>
> **cloned_report** (str)
>
>> Required; Name of the new Power BI report.
>
> **workspace** (str, default=None)
>
>> Optional; The Fabric workspace name.
Defaults to None which resolves to the workspace of the attached lakehouse
or if no lakehouse attached, resolves to the workspace of the notebook.
>
> **target_workspace** (str, default=None)
>
>> Optional; The name of the Fabric workspace to place the cloned report.
Defaults to None which resolves to the workspace of the attached lakehouse
or if no lakehouse attached, resolves to the workspace of the notebook.
>
> **target_dataset** (str, default=None)
>
>> Optional; The name of the semantic model to be used by the cloned report.
Defaults to None which resolves to the semantic model used by the initial report.
>
> **target_dataset_workspace** (str, default=None)
>
>> Optional; The workspace in which the semantic model to be used by the report resides.
Defaults to None which resolves to the semantic model used by the initial report.
>
### [create_model_bpa_report](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.report.html#sempy_labs.report.create_model_bpa_report)
#### Dynamically generates a Best Practice Analyzer report for analyzing semantic models.

```python
import sempy_labs as labs
import sempy_labs.report as rep
rep.create_model_bpa_report(
    report='ModelBPA', # This parameter is optional
    dataset='ModelBPA', # This parameter is optional
    dataset_workspace=None, # This parameter is optional
)
```

### Parameters
> **report** (str, default='ModelBPA')
>
>> Optional; Name of the report.
Defaults to 'ModelBPA'.
>
> **dataset** (str, default='ModelBPA')
>
>> Optional; Name of the semantic model which feeds this report.
Defaults to 'ModelBPA'
>
> **dataset_workspace** (str, default=None)
>
>> Optional; The Fabric workspace name in which the semantic model resides.
Defaults to None which resolves to the workspace of the attached lakehouse
or if no lakehouse attached, resolves to the workspace of the notebook.
>
### [create_report_from_reportjson](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.report.html#sempy_labs.report.create_report_from_reportjson)
#### Creates a report based on a report.json file (and an optional themes.json file).

```python
import sempy_labs as labs
import sempy_labs.report as rep
rep.create_report_from_reportjson(
    report='',
    dataset='AdvWorks',
    report_json='',
    theme_json=None, # This parameter is optional
    workspace=None, # This parameter is optional
)
```

### Parameters
> **report** (str)
>
>> Required; Name of the report.
>
> **dataset** (str)
>
>> Required; Name of the semantic model to connect to the report.
>
> **report_json** (dict)
>
>> Required; The report.json file to be used to create the report.
>
> **theme_json** (dict, default=None)
>
>> Optional; The theme.json file to be used for the theme of the report.
>
> **workspace** (str, default=None)
>
>> Optional; The Fabric workspace name.
Defaults to None which resolves to the workspace of the attached lakehouse
or if no lakehouse attached, resolves to the workspace of the notebook.
>
### [export_report](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.report.html#sempy_labs.report.export_report)
#### Exports a Power BI report to a file in your lakehouse.

```python
import sempy_labs as labs
import sempy_labs.report as rep
rep.export_report(
    report='',
    export_format='',
    file_name=None, # This parameter is optional
    bookmark_name=None, # This parameter is optional
    page_name=None, # This parameter is optional
    visual_name=None, # This parameter is optional
    report_filter=None, # This parameter is optional
    workspace=None, # This parameter is optional
)
```

### Parameters
> **report** (str)
>
>> Required; Name of the Power BI report.
>
> **export_format** (str)
>
>> Required; The format in which to export the report. For image formats, enter the file extension in this parameter, not 'IMAGE'.
[Valid formats](https://learn.microsoft.com/rest/api/power-bi/reports/export-to-file-in-group#fileformat)
>
> **file_name** (str, default=None)
>
>> Optional; The name of the file to be saved within the lakehouse. Do not include the file extension. Defaults ot the reportName parameter value.
>
> **bookmark_name** (str, default=None)
>
>> Optional; The name (GUID) of a bookmark within the report.
>
> **page_name** (str, default=None)
>
>> Optional; The name (GUID) of the report page.
>
> **visual_name** (str, default=None)
>
>> Optional; The name (GUID) of a visual. If you specify this parameter you must also specify the page_name parameter.
>
> **report_filter** (str, default=None)
>
>> Optional; A report filter to be applied when exporting the report. Syntax is user-friendly. See above for examples.
>
> **workspace** (str, default=None)
>
>> Optional; The Fabric workspace name.
Defaults to None which resolves to the workspace of the attached lakehouse
or if no lakehouse attached, resolves to the workspace of the notebook.
>
### [get_report_definition](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.report.html#sempy_labs.report.get_report_definition)
#### Gets the collection of definition files of a report.

```python
import sempy_labs as labs
import sempy_labs.report as rep
rep.get_report_definition(
    report='',
    workspace=None, # This parameter is optional
)
```

### Parameters
> **report** (str)
>
>> Required; Name of the report.
>
> **workspace** (str, default=None)
>
>> Optional; The Fabric workspace name in which the report resides.
Defaults to None which resolves to the workspace of the attached lakehouse
or if no lakehouse attached, resolves to the workspace of the notebook.
>
### Returns
> pandas.DataFrame; The collection of report definition files within a pandas dataframe.
### [get_report_json](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.report.html#sempy_labs.report.get_report_json)
#### Gets the report.json file content of a Power BI report.

```python
import sempy_labs as labs
import sempy_labs.report as rep
rep.get_report_json(
    report='',
    workspace=None, # This parameter is optional
    save_to_file_name=None, # This parameter is optional
)
```

### Parameters
> **report** (str)
>
>> Required; Name of the Power BI report.
>
> **workspace** (str, default=None)
>
>> Optional; The Fabric workspace name in which the report exists.
Defaults to None which resolves to the workspace of the attached lakehouse
or if no lakehouse attached, resolves to the workspace of the notebook.
>
> **save_to_file_name** (str, default=None)
>
>> Optional; Specifying this parameter will save the report.json file to the lakehouse attached to the notebook with the file name of this parameter.
>
### Returns
> dict; The report.json file for a given Power BI report.
### [launch_report](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.report.html#sempy_labs.report.launch_report)
#### Shows a Power BI report within a Fabric notebook.

```python
import sempy_labs as labs
import sempy_labs.report as rep
rep.launch_report(
    report='',
    workspace=None, # This parameter is optional
)
```

### Parameters
> **report** (str)
>
>> Required; Name of the Power BI report.
>
> **workspace** (str, default=None)
>
>> Optional; The Fabric workspace name.
Defaults to None which resolves to the workspace of the attached lakehouse
or if no lakehouse attached, resolves to the workspace of the notebook.
>
### Returns
> str; An embedded Power BI report within the notebook.
### [report_rebind](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.report.html#sempy_labs.report.report_rebind)
#### Rebinds a report to a semantic model.

```python
import sempy_labs as labs
import sempy_labs.report as rep
rep.report_rebind(
    report='',
    dataset='AdvWorks',
    report_workspace=None, # This parameter is optional
    dataset_workspace=None, # This parameter is optional
)
```

### Parameters
> **report** (str | List[str])
>
>> Required; Name(s) of the Power BI report(s).
>
> **dataset** (str)
>
>> Required; Name of the semantic model.
>
> **report_workspace** (str, default=None)
>
>> Optional; The name of the Fabric workspace in which the report resides.
Defaults to None which resolves to the workspace of the attached lakehouse
or if no lakehouse attached, resolves to the workspace of the notebook.
>
> **dataset_workspace** (str, default=None)
>
>> Optional; The name of the Fabric workspace in which the semantic model resides.
Defaults to None which resolves to the workspace of the attached lakehouse
or if no lakehouse attached, resolves to the workspace of the notebook.
>
### [report_rebind_all](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.report.html#sempy_labs.report.report_rebind_all)
#### Rebinds all reports across all workspaces which are bound to a specific semantic model to a new semantic model.

```python
import sempy_labs as labs
import sempy_labs.report as rep
rep.report_rebind_all(
    dataset='AdvWorks',
    new_dataset='',
    dataset_workspace=None, # This parameter is optional
    new_dataset_workpace=None, # This parameter is optional
    report_workspace=None, # This parameter is optional
)
```

### Parameters
> **dataset** (str)
>
>> Required; Name of the semantic model currently binded to the reports.
>
> **new_dataset** (str)
>
>> Required; Name of the semantic model to rebind to the reports.
>
> **dataset_workspace** (str, default=None)
>
>> Optional; The name of the Fabric workspace in which the original semantic model resides.
Defaults to None which resolves to the workspace of the attached lakehouse
or if no lakehouse attached, resolves to the workspace of the notebook.
>
> **new_dataset_workspace** (str, default=None)
>
>> Optional; The name of the Fabric workspace in which the new semantic model resides.
Defaults to None which resolves to the workspace of the attached lakehouse
or if no lakehouse attached, resolves to the workspace of the notebook.
>
> **report_workspace** (str | List[str], default=None)
>
>> Optional; The name(s) of the Fabric workspace(s) in which the report(s) reside(s).
Defaults to None which finds all reports in all workspaces which use the semantic model and rebinds them to
the new semantic model.
>
### [update_report_from_reportjson](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.report.html#sempy_labs.report.update_report_from_reportjson)
#### Updates a report based on a report.json file.

```python
import sempy_labs as labs
import sempy_labs.report as rep
rep.update_report_from_reportjson(
    report='',
    report_json='',
    workspace=None, # This parameter is optional
)
```

### Parameters
> **report** (str)
>
>> Required; Name of the report.
>
> **report_json** (dict)
>
>> Required; The report.json file to be used to update the report.
>
> **workspace** (str, default=None)
>
>> Optional; The Fabric workspace name in which the report resides.
Defaults to None which resolves to the workspace of the attached lakehouse
or if no lakehouse attached, resolves to the workspace of the notebook.
>
### [add_calculated_column](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.add_calculated_column)
#### Adds a calculated column to a table within a semantic model.

```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.add_calculated_column(
        table_name='',
        column_name='',
        expression='',
        data_type='',
        format_string=None, # This parameter is optional
        hidden=False, # This parameter is optional
        description=None, # This parameter is optional
        display_folder=None, # This parameter is optional
        data_category=None, # This parameter is optional
        key=False, # This parameter is optional
        summarize_by=None, # This parameter is optional
)
```

### Parameters
> **table_name** (str)
>
>> Required; Name of the table in which the column will be created.
>
> **column_name** (str)
>
>> Required; Name of the column.
>
> **expression** (str)
>
>> Required; The DAX expression for the column.
>
> **data_type** (str)
>
>> Required; The data type of the column.
>
> **format_string** (str, default=None)
>
>> Optional; Format string of the column.
>
> **hidden** (bool, default=False)
>
>> Optional; Whether the column will be hidden or visible.
>
> **description** (str, default=None)
>
>> Optional; A description of the column.
>
> **display_folder** (str, default=None)
>
>> Optional; The display folder in which the column will reside.
>
> **data_category** (str, default=None)
>
>> Optional; The data category of the column.
>
> **key** (bool, default=False)
>
>> Optional; Marks the column as the primary key of the table.
>
> **summarize_by** (str, default=None)
>
>> Optional; Sets the value for the Summarize By property of the column.
Defaults to None which resolves to 'Default'.
>
### [add_calculated_table](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.add_calculated_table)
#### Adds a calculated table to the semantic model.

```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.add_calculated_table(
        name='',
        expression='',
        description=None, # This parameter is optional
        data_category=None, # This parameter is optional
        hidden=False, # This parameter is optional
)
```

### Parameters
> **name** (str)
>
>> Required; Name of the table.
>
> **expression** (str)
>
>> Required; The DAX expression for the calculated table.
>
> **description** (str, default=None)
>
>> Optional; A description of the table.
>
> **data_catgegory** (str, default=None)
>
>> Optional; The data category for the table.
>
> **hidden** (bool, default=False)
>
>> Optional; Whether the table is hidden or visible.
>
### [add_calculated_table_column](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.add_calculated_table_column)
#### Adds a calculated table column to a calculated table within a semantic model.

```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.add_calculated_table_column(
        table_name='',
        column_name='',
        source_column='',
        data_type='',
        format_string=None, # This parameter is optional
        hidden=False, # This parameter is optional
        description=None, # This parameter is optional
        display_folder=None, # This parameter is optional
        data_category=None, # This parameter is optional
        key=False, # This parameter is optional
        summarize_by=None, # This parameter is optional
)
```

### Parameters
> **table_name** (str)
>
>> Required; Name of the table in which the column will be created.
>
> **column_name** (str)
>
>> Required; Name of the column.
>
> **source_column** (str)
>
>> Required; The source column for the column.
>
> **data_type** (str)
>
>> Required; The data type of the column.
>
> **format_string** (str, default=None)
>
>> Optional; Format string of the column.
>
> **hidden** (bool, default=False)
>
>> Optional; Whether the column will be hidden or visible.
>
> **description** (str, default=None)
>
>> Optional; A description of the column.
>
> **display_folder** (str, default=None)
>
>> Optional; The display folder in which the column will reside.
>
> **data_category** (str, default=None)
>
>> Optional; The data category of the column.
>
> **key** (bool, default=False)
>
>> Optional; Marks the column as the primary key of the table.
>
> **summarize_by** (str, default=None)
>
>> Optional; Sets the value for the Summarize By property of the column.
Defaults to None resolves to 'Default'.
>
### [add_calculation_group](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.add_calculation_group)
#### Adds a [calculation group](https://learn.microsoft.com/dotnet/api/microsoft.analysisservices.tabular.calculationgroup?view=analysisservices-dotnet) to a semantic model.

```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.add_calculation_group(
        name='',
        precedence='',
        description=None, # This parameter is optional
        hidden=False, # This parameter is optional
)
```

### Parameters
> **name** (str)
>
>> Required; Name of the calculation group.
>
> **precedence** (int)
>
>> Required; The precedence of the calculation group.
>
> **description** (str, default=None)
>
>> Optional; A description of the calculation group.
>
> **hidden** (bool, default=False)
>
>> Optional; Whether the calculation group is hidden/visible.
>
### [add_calculation_item](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.add_calculation_item)
#### Adds a [calculation item](https://learn.microsoft.com/dotnet/api/microsoft.analysisservices.tabular.calculationitem?view=analysisservices-dotnet) to
a [calculation group](https://learn.microsoft.com/dotnet/api/microsoft.analysisservices.tabular.calculationgroup?view=analysisservices-dotnet) within a semantic model.
```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.add_calculation_item(
        table_name='',
        calculation_item_name='',
        expression='',
        ordinal=None, # This parameter is optional
        description=None, # This parameter is optional
        format_string_expression=None, # This parameter is optional
)
```

### Parameters
> **table_name** (str)
>
>> Required; Name of the table in which the calculation item will be created.
>
> **calculation_item_name** (str)
>
>> Required; Name of the calculation item.
>
> **expression** (str)
>
>> Required; The DAX expression for the calculation item.
>
> **ordinal** (int, default=None)
>
>> Optional; The ordinal of the calculation item.
>
> **format_string_expression** (str, default=None)
>
>> Optional; The format string expression for the calculation item.
>
> **description** (str, default=None)
>
>> Optional; A description of the calculation item.
>
### [add_data_column](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.add_data_column)
#### Adds a data column to a table within a semantic model.

```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.add_data_column(
        table_name='',
        column_name='',
        source_column='',
        data_type='',
        format_string=None, # This parameter is optional
        hidden=False, # This parameter is optional
        description=None, # This parameter is optional
        display_folder=None, # This parameter is optional
        data_category=None, # This parameter is optional
        key=False, # This parameter is optional
        summarize_by=None, # This parameter is optional
)
```

### Parameters
> **table_name** (str)
>
>> Required; Name of the table in which the column will be created.
>
> **column_name** (str)
>
>> Required; Name of the column.
>
> **source_column** (str)
>
>> Required; The source column for the column.
>
> **data_type** (str)
>
>> Required; The data type of the column.
>
> **format_string** (str, default=None)
>
>> Optional; Format string of the column.
>
> **hidden** (bool, default=False)
>
>> Optional; Whether the column will be hidden or visible.
>
> **description** (str, default=None)
>
>> Optional; A description of the column.
>
> **display_folder** (str, default=None)
>
>> Optional; The display folder in which the column will reside.
>
> **data_category** (str, default=None)
>
>> Optional; The data category of the column.
>
> **key** (bool, default=False)
>
>> Optional; Marks the column as the primary key of the table.
>
> **summarize_by** (str, default=None)
>
>> Optional; Sets the value for the Summarize By property of the column.
Defaults to None resolves to 'Default'.
>
### [add_entity_partition](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.add_entity_partition)
#### Adds an entity partition to a table within a semantic model.

```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.add_entity_partition(
        table_name='',
        entity_name='',
        expression=None, # This parameter is optional
        description=None, # This parameter is optional
)
```

### Parameters
> **table_name** (str)
>
>> Required; Name of the table.
>
> **entity_name** (str)
>
>> Required; Name of the lakehouse table.
>
> **expression** (TOM Object, default=None)
>
>> Optional; The expression used by the table.
Defaults to None which resolves to the 'DatabaseQuery' expression.
>
> **description** (str, default=None)
>
>> Optional; A description for the partition.
>
### [add_expression](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.add_expression)
#### Adds an [expression](https://learn.microsoft.com/dotnet/api/microsoft.analysisservices.tabular.namedexpression?view=analysisservices-dotnet) to a semantic model.

```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.add_expression(
        name='',
        expression='',
        description=None, # This parameter is optional
)
```

### Parameters
> **name** (str)
>
>> Required; Name of the expression.
>
> **expression** (str)
>
>> Required; The M expression of the expression.
>
> **description** (str, default=None)
>
>> Optional; A description of the expression.
>
### [add_field_parameter](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.add_field_parameter)
#### Adds a [field parameter](https://learn.microsoft.com/power-bi/create-reports/power-bi-field-parameters) to the semantic model.

```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.add_field_parameter(
        table_name='',
        objects='',
        object_names=None, # This parameter is optional
)
```

### Parameters
> **table_name** (str)
>
>> Required; Name of the table.
>
> **objects** (List[str])
>
>> Required; The columns/measures to be included in the field parameter.
Columns must be specified as such : 'Table Name'[Column Name].
Measures may be formatted as '[Measure Name]' or 'Measure Name'.
>
> **object_names** (List[str], default=None)
>
>> Optional; The corresponding visible name for the measures/columns in the objects list.
Defaults to None which shows the measure/column name.
>
### [add_hierarchy](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.add_hierarchy)
#### Adds a [hierarchy](https://learn.microsoft.com/dotnet/api/microsoft.analysisservices.hierarchy?view=analysisservices-dotnet) to a table within a semantic model.

```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.add_hierarchy(
        table_name='',
        hierarchy_name='',
        columns='',
        levels=None, # This parameter is optional
        hierarchy_description=None, # This parameter is optional
        hierarchy_hidden=False, # This parameter is optional
)
```

### Parameters
> **table_name** (str)
>
>> Required; Name of the table.
>
> **hierarchy_name** (str)
>
>> Required; Name of the hierarchy.
>
> **columns** (List[str])
>
>> Required; Names of the columns to use within the hierarchy.
>
> **levels** (List[str], default=None)
>
>> Optional; Names of the levels to use within the hierarhcy (instead of the column names).
>
> **hierarchy_description** (str, default=None)
>
>> Optional; A description of the hierarchy.
>
> **hierarchy_hidden** (bool, default=False)
>
>> Optional; Whether the hierarchy is visible or hidden.
>
### [add_incremental_refresh_policy](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.add_incremental_refresh_policy)
#### Adds an [incremental refresh](https://learn.microsoft.com/power-bi/connect-data/incremental-refresh-overview) policy for a table within a semantic model.

```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.add_incremental_refresh_policy(
        table_name='',
        column_name='',
        start_date='',
        end_date='',
        incremental_granularity='',
        incremental_periods='',
        rolling_window_granularity='',
        rolling_window_periods='',
        only_refresh_complete_days=False, # This parameter is optional
        detect_data_changes_column=None, # This parameter is optional
)
```

### Parameters
> **table_name** (str)
>
>> Required; Name of the table.
>
> **column_name** (str)
>
>> Required; The DateTime column to be used for the RangeStart and RangeEnd parameters.
>
> **start_date** (str)
>
>> Required; The date to be used for the RangeStart parameter.
>
> **end_date** (str)
>
>> Required; The date to be used for the RangeEnd parameter.
>
> **incremental_granularity** (str)
>
>> Required; Granularity of the (most recent) incremental refresh range.
>
> **incremental_periods** (int)
>
>> Required; Number of periods for the incremental refresh range.
>
> **rolling_window_granularity** (str)
>
>> Required; Target granularity of the rolling window for the whole semantic model.
>
> **rolling_window_periods** (int)
>
>> Required; Number of periods for the rolling window for the whole semantic model.
>
> **only_refresh_complete_days** (bool, default=False)
>
>> Optional; Lag or leading periods from Now() to the rolling window head.
>
> **detect_data_changes_column** (str, default=None)
>
>> Optional; The column to use for detecting data changes.
Defaults to None which resolves to not detecting data changes.
>
### [add_m_partition](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.add_m_partition)
#### Adds an M-partition to a table within a semantic model.

```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.add_m_partition(
        table_name='',
        partition_name='',
        expression='',
        mode=None, # This parameter is optional
        description=None, # This parameter is optional
)
```

### Parameters
> **table_name** (str)
>
>> Required; Name of the table.
>
> **partition_name** (str)
>
>> Required; Name of the partition.
>
> **expression** (str)
>
>> Required; The M expression encapsulating the logic for the partition.
>
> **mode** (str, default=None)
>
>> Optional; The query mode for the partition.
Defaults to None which resolves to 'Import'.
[Valid mode values](https://learn.microsoft.com/en-us/dotnet/api/microsoft.analysisservices.tabular.modetype?view=analysisservices-dotnet)
>
> **description** (str, default=None)
>
>> Optional; A description for the partition.
>
### [add_measure](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.add_measure)
#### Adds a measure to the semantic model.

```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.add_measure(
        table_name='',
        measure_name='',
        expression='',
        format_string=None, # This parameter is optional
        hidden=False, # This parameter is optional
        description=None, # This parameter is optional
        display_folder=None, # This parameter is optional
        format_string_expression=None, # This parameter is optional
)
```

### Parameters
> **table_name** (str)
>
>> Required; Name of the table in which the measure will be created.
>
> **measure_name** (str)
>
>> Required; Name of the measure.
>
> **expression** (str)
>
>> Required; DAX expression of the measure.
>
> **format_string** (str, default=None)
>
>> Optional; Format string of the measure.
>
> **hidden** (bool, default=False)
>
>> Optional; Whether the measure will be hidden or visible.
>
> **description** (str, default=None)
>
>> Optional; A description of the measure.
>
> **display_folder** (str, default=None)
>
>> Optional; The display folder in which the measure will reside.
>
> **format_string_expression** (str, default=None)
>
>> Optional; The format string expression.
>
### [add_perspective](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.add_perspective)
#### Adds a [perspective](https://learn.microsoft.com/dotnet/api/microsoft.analysisservices.perspective?view=analysisservices-dotnet) to a semantic model.

```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.add_perspective(
        perspective_name='',
)
```

### Parameters
> **perspective_name** (str)
>
>> Required; Name of the perspective.
>
### [add_relationship](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.add_relationship)
#### Adds a [relationship](https://learn.microsoft.com/dotnet/api/microsoft.analysisservices.tabular.singlecolumnrelationship?view=analysisservices-dotnet) to a semantic model.

```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.add_relationship(
        from_table='',
        from_column='',
        to_table='',
        to_column='',
        from_cardinality='',
        to_cardinality='',
        cross_filtering_behavior=None, # This parameter is optional
        is_active=True, # This parameter is optional
        security_filtering_behavior=None, # This parameter is optional
        rely_on_referential_integrity=False, # This parameter is optional
)
```

### Parameters
> **from_table** (str)
>
>> Required; Name of the table on the 'from' side of the relationship.
>
> **from_column** (str)
>
>> Required; Name of the column on the 'from' side of the relationship.
>
> **to_table** (str)
>
>> Required; Name of the table on the 'to' side of the relationship.
>
> **to_column** (str)
>
>> Required; Name of the column on the 'to' side of the relationship.
>
> **from_cardinality** (str)
>
>> Required; The cardinality of the 'from' side of the relationship. Options: ['Many', 'One', 'None'].
>
> **to_cardinality** (str)
>
>> Required; The cardinality of the 'to' side of the relationship. Options: ['Many', 'One', 'None'].
>
> **cross_filtering_behavior** (str, default=None)
>
>> Optional; Setting for the cross filtering behavior of the relationship. Options: ('Automatic', 'OneDirection', 'BothDirections').
Defaults to None which resolves to 'Automatic'.
>
> **is_active** (bool, default=True)
>
>> Optional; Setting for whether the relationship is active or not.
>
> **security_filtering_behavior** (str, default=None)
>
>> Optional; Setting for the security filtering behavior of the relationship. Options: ('None', 'OneDirection', 'BothDirections').
Defaults to None which resolves to 'OneDirection'.
>
> **rely_on_referential_integrity** (bool, default=False)
>
>> Optional; Setting for the rely on referential integrity of the relationship.
>
### [add_role](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.add_role)
#### Adds a role to a semantic model.

```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.add_role(
        role_name='',
        model_permission=None, # This parameter is optional
        description=None, # This parameter is optional
)
```

### Parameters
> **role_name** (str)
>
>> Required; Name of the role.
>
> **model_permission** (str, default=None)
>
>> Optional; The model permission for the role.
Defaults to None which resolves to 'Read'.
>
> **description** (str, default=None)
>
>> Optional; A description of the role.
>
### [add_table](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.add_table)
#### Adds a table to the semantic model.

```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.add_table(
        name='',
        description=None, # This parameter is optional
        data_category=None, # This parameter is optional
        hidden=False, # This parameter is optional
)
```

### Parameters
> **name** (str)
>
>> Required; Name of the table.
>
> **description** (str, default=None)
>
>> Optional; A description of the table.
>
> **data_catgegory** (str, default=None)
>
>> Optional; The data category for the table.
>
> **hidden** (bool, default=False)
>
>> Optional; Whether the table is hidden or visible.
>
### [add_time_intelligence](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.add_time_intelligence)
#### Adds time intelligence measures

```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.add_time_intelligence(
        measure_name='',
        date_table='',
        time_intel='',
)
```

### Parameters
> **measure_name** (str)
>
>> Required; Name of the measure
>
> **date_table** (str)
>
>> Required; Name of the date table.
>
> **time_intel** (str, List[str])
>
>> Required; Time intelligence measures to create (i.e. MTD, YTD, QTD).
>
### [add_to_perspective](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.add_to_perspective)
#### Adds an object to a [perspective](https://learn.microsoft.com/dotnet/api/microsoft.analysisservices.perspective?view=analysisservices-dotnet).

```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.add_to_perspective(
        object=,
        perspective_name='',
)
```

### Parameters
> **object** (TOM Object)
>
>> Required; An object (i.e. table/column/measure) within a semantic model.
>
> **perspective_name** (str)
>
>> Required; Name of the perspective.
>
### [add_translation](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.add_translation)
#### Adds a [translation language](https://learn.microsoft.com/dotnet/api/microsoft.analysisservices.tabular.culture?view=analysisservices-dotnet) (culture) to a semantic model.

```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.add_translation(
        language='',
)
```

### Parameters
> **language** (str)
>
>> Required; The language code (i.e. 'it-IT' for Italian).
>
### [all_calculated_columns](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.all_calculated_columns)
#### Outputs a list of all calculated columns within all tables in the semantic model.

```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.all_calculated_columns()
```

### Returns
> Iterator[Microsoft.AnalysisServices.Tabular.Column]; All calculated columns within the semantic model.
### [all_calculated_tables](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.all_calculated_tables)
#### Outputs a list of all calculated tables in the semantic model.

```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.all_calculated_tables()
```

### Returns
> Iterator[Microsoft.AnalysisServices.Tabular.Table]; All calculated tables within the semantic model.
### [all_calculation_groups](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.all_calculation_groups)
#### Outputs a list of all calculation groups in the semantic model.

```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.all_calculation_groups()
```

### Returns
> Iterator[Microsoft.AnalysisServices.Tabular.Table]; All calculation groups within the semantic model.
### [all_calculation_items](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.all_calculation_items)
#### Outputs a list of all calculation items in the semantic model.

```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.all_calculation_items()
```

### Returns
> Iterator[Microsoft.AnalysisServices.Tabular.CalculationItem]; All calculation items within the semantic model.
### [all_columns](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.all_columns)
#### Outputs a list of all columns within all tables in the semantic model.

```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.all_columns()
```

### Returns
> Iterator[Microsoft.AnalysisServices.Tabular.Column]; All columns within the semantic model.
### [all_date_tables](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.all_date_tables)
#### Outputs the tables which are marked as [date tables](https://learn.microsoft.com/power-bi/transform-model/desktop-date-tables) within a semantic model.

```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.all_date_tables()
```

### Returns
> Microsoft.AnalysisServices.Tabular.TableCollection; All tables marked as date tables within a semantic model.
### [all_hierarchies](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.all_hierarchies)
#### Outputs a list of all hierarchies in the semantic model.

```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.all_hierarchies()
```

### Returns
> Iterator[Microsoft.AnalysisServices.Tabular.Hierarchy]; All hierarchies within the semantic model.
### [all_hybrid_tables](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.all_hybrid_tables)
#### Outputs the [hybrid tables](https://learn.microsoft.com/power-bi/connect-data/service-dataset-modes-understand#hybrid-tables) within a semantic model.

```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.all_hybrid_tables()
```

### Returns
> Microsoft.AnalysisServices.Tabular.TableCollection; All hybrid tables within a semantic model.
### [all_levels](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.all_levels)
#### Outputs a list of all levels in the semantic model.

```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.all_levels()
```

### Returns
> Iterator[Microsoft.AnalysisServices.Tabular.Level]; All levels within the semantic model.
### [all_measures](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.all_measures)
#### Outputs a list of all measures in the semantic model.

```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.all_measures()
```

### Returns
> Iterator[Microsoft.AnalysisServices.Tabular.Measure]; All measures within the semantic model.
### [all_partitions](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.all_partitions)
#### Outputs a list of all partitions in the semantic model.

```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.all_partitions()
```

### Returns
> Iterator[Microsoft.AnalysisServices.Tabular.Partition]; All partitions within the semantic model.
### [all_rls](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.all_rls)
#### Outputs a list of all row level security expressions in the semantic model.

```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.all_rls()
```

### Returns
> Iterator[Microsoft.AnalysisServices.Tabular.TablePermission]; All row level security expressions within the semantic model.
### [apply_refresh_policy](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.apply_refresh_policy)
#### [Applies the incremental refresh](https://learn.microsoft.com/dotnet/api/microsoft.analysisservices.tabular.table.applyrefreshpolicy?view=analysisservices-dotnet#microsoft-analysisservices-tabular-table-applyrefreshpolicy(system-boolean-system-int32)) policy for a table within a semantic model.

```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.apply_refresh_policy(
        table_name='',
        effective_date=None, # This parameter is optional
        refresh=True, # This parameter is optional
        max_parallelism=0, # This parameter is optional
)
```

### Parameters
> **table_name** (str)
>
>> Required; Name of the table.
>
> **effective_date** (DateTime, default=None)
>
>> Optional; The effective date that is used when calculating the partitioning scheme.
>
> **refresh** (bool, default=True)
>
>> Optional; An indication if partitions of the table should be refreshed or not; the default behavior is to do the refresh.
>
> **max_parallelism** (int, default=0)
>
>> Optional; The degree of parallelism during the refresh execution.
>
### [cardinality](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.cardinality)
#### Obtains the cardinality of a column within a semantic model.

```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.cardinality(
        column=tom.model.Tables["Geography"].Columns["GeographyKey"],
)
```

### Parameters
> **column** (TOM Object)
>
>> Required; The column object within the semantic model.
>
### Returns
> int; Cardinality of the TOM column.
### [clear_annotations](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.clear_annotations)
#### Removes all [annotations](https://learn.microsoft.com/dotnet/api/microsoft.analysisservices.annotation?view=analysisservices-dotnet) on an object within the semantic model.

```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.clear_annotations(
        object=,
)
```

### Parameters
> **object** (TOM Object)
>
>> Required; An object (i.e. table/column/measure) within a semantic model.
>
### [clear_extended_properties](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.clear_extended_properties)
#### Removes all [extended properties](https://learn.microsoft.com/dotnet/api/microsoft.analysisservices.tabular.extendedproperty?view=analysisservices-dotnet) on an object within the semantic model.

```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.clear_extended_properties(
        object=,
)
```

### Parameters
> **object** (TOM Object)
>
>> Required; An object (i.e. table/column/measure) within a semantic model.
>
### [data_size](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.data_size)
#### Obtains the data size of a column within a semantic model.

```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.data_size(
        column=tom.model.Tables["Geography"].Columns["GeographyKey"],
)
```

### Parameters
> **column** (TOM Object)
>
>> Required; The column object within the semantic model.
>
### Returns
> int; Data size of the TOM column.
### [depends_on](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.depends_on)
#### Obtains the objects on which the specified object depends.

```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.depends_on(
        object=,
        dependencies=labs.get_model_calc_dependencies(dataset=tom._dataset, workspace=tom._workspace),
)
```

### Parameters
> **object** (TOM Object)
>
>> Required; The TOM object within the semantic model.
>
> **dependencies** (pandas.DataFrame)
>
>> Required; A pandas dataframe with the output of the 'get_model_calc_dependencies' function.
>
### Returns
> Microsoft.AnalysisServices.Tabular.TableCollection, Microsoft.AnalysisServices.Tabular.ColumnCollection, Microsoft.AnalysisServices.Tabular.MeasureCollection; Objects on which the specified object depends.
### [dictionary_size](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.dictionary_size)
#### Obtains the dictionary size of a column within a semantic model.

```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.dictionary_size(
        column=tom.model.Tables["Geography"].Columns["GeographyKey"],
)
```

### Parameters
> **column** (TOM Object)
>
>> Required; The column object within the semantic model.
>
### Returns
> int; Dictionary size of the TOM column.
### [fully_qualified_measures](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.fully_qualified_measures)
#### Obtains all fully qualified measure references for a given object.

```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.fully_qualified_measures(
        object=tom.model.Tables["Sales"].Measures["Sales Amount"],
        dependencies=labs.get_model_calc_dependencies(dataset=tom._dataset, workspace=tom._workspace),
)
```

### Parameters
> **object** (TOM Object)
>
>> Required; The TOM object within the semantic model.
>
> **dependencies** (pandas.DataFrame)
>
>> Required; A pandas dataframe with the output of the 'get_model_calc_dependencies' function.
>
### Returns
> Microsoft.AnalysisServices.Tabular.MeasureCollection; All fully qualified measure references for a given object.
### [get_annotation_value](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.get_annotation_value)
#### Obtains the [annotation](https://learn.microsoft.com/dotnet/api/microsoft.analysisservices.annotation?view=analysisservices-dotnet) value for a given annotation on an object within the semantic model.

```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.get_annotation_value(
        object=,
        name='',
)
```

### Parameters
> **object** (TOM Object)
>
>> Required; An object (i.e. table/column/measure) within a semantic model.
>
> **name** (str)
>
>> Required; Name of the annotation.
>
### Returns
> str; The annotation value.
### [get_annotations](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.get_annotations)
#### Shows all [annotations](https://learn.microsoft.com/dotnet/api/microsoft.analysisservices.annotation?view=analysisservices-dotnet) for a given object within a semantic model.

```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.get_annotations(
        object=,
)
```

### Parameters
> **object** (TOM Object)
>
>> Required; An object (i.e. table/column/measure) within a semantic model.
>
### Returns
> Microsoft.AnalysisServices.Tabular.Annotation; TOM objects of all the annotations on a particular object within the semantic model.
### [get_extended_properties](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.get_extended_properties)
#### Retrieves all [extended properties](https://learn.microsoft.com/dotnet/api/microsoft.analysisservices.tabular.extendedproperty?view=analysisservices-dotnet) on an object within the semantic model.

```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.get_extended_properties(
        object=,
)
```

### Parameters
> **object** (TOM Object)
>
>> Required; An object (i.e. table/column/measure) within a semantic model.
>
### Returns
> Microsoft.AnalysisServices.Tabular.ExtendedPropertiesCollection; TOM Objects of all the extended properties.
### [get_extended_property_value](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.get_extended_property_value)
#### Retrieves the value of an [extended property](https://learn.microsoft.com/dotnet/api/microsoft.analysisservices.tabular.extendedproperty?view=analysisservices-dotnet) for an object within the semantic model.

```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.get_extended_property_value(
        object=,
        name='',
)
```

### Parameters
> **object** (TOM Object)
>
>> Required; An object (i.e. table/column/measure) within a semantic model.
>
> **name** (str)
>
>> Required; Name of the annotation.
>
### Returns
> str; The extended property value.
### [has_aggs](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.has_aggs)
#### Identifies if a semantic model has any [aggregations](https://learn.microsoft.com/power-bi/transform-model/aggregations-advanced).

```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.has_aggs()
```

### Returns
> bool; Indicates if the semantic model has any aggregations.
### [has_date_table](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.has_date_table)
#### Identifies if a semantic model has a table marked as a [date table](https://learn.microsoft.com/power-bi/transform-model/desktop-date-tables).

```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.has_date_table()
```

### Returns
> bool; Indicates if the semantic model has a table marked as a date table.
### [has_hybrid_table](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.has_hybrid_table)
#### Identifies if a semantic model has a [hybrid table](https://learn.microsoft.com/power-bi/connect-data/service-dataset-modes-understand#hybrid-tables).

```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.has_hybrid_table()
```

### Returns
> bool; Indicates if the semantic model has a hybrid table.
### [has_incremental_refresh_policy](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.has_incremental_refresh_policy)
#### Identifies whether a table has an [incremental refresh](https://learn.microsoft.com/power-bi/connect-data/incremental-refresh-overview) policy.

```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.has_incremental_refresh_policy(
        table_name='',
)
```

### Parameters
> **table_name** (str)
>
>> Required; Name of the table.
>
### Returns
> bool; An indicator whether a table has an incremental refresh policy.
### [in_perspective](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.in_perspective)
#### Indicates whether an object is contained within a given [perspective](https://learn.microsoft.com/dotnet/api/microsoft.analysisservices.perspective?view=analysisservices-dotnet).

```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.in_perspective(
        object=,
        perspective_name='',
)
```

### Parameters
> **object** (TOM Object)
>
>> Required; An object (i.e. table/column/measure) within a semantic model.
>
> **perspecitve_name** (str)
>
>> Required; Name of the perspective.
>
### Returns
> bool; An indication as to whether the object is contained within the given perspective.
### [is_agg_table](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.is_agg_table)
#### Identifies if a table has [aggregations](https://learn.microsoft.com/power-bi/transform-model/aggregations-advanced).

```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.is_agg_table(
        table_name='',
)
```

### Parameters
> **table_name** (str)
>
>> Required; Name of the table.
>
### Returns
> bool; Indicates if the table has any aggregations.
### [is_auto_date_table](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.is_auto_date_table)
#### Identifies if a table is an `auto date/time table <https://learn.microsoft.com/power-bi/transform-model/desktop-auto-date-time>`_.

```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.is_auto_date_table(
        table_name='',
)
```

### Parameters
> **table_name** (str)
>
>> Required; Name of the table.
>
### Returns
> bool; Indicates if the table is an auto-date table.
### [is_calculated_table](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.is_calculated_table)
#### Identifies if a table is a calculated table.

```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.is_calculated_table(
        table_name='',
)
```

### Parameters
> **table_name** (str)
>
>> Required; Name of the table.
>
### Returns
> bool; A boolean value indicating whether the table is a calculated table.
### [is_date_table](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.is_date_table)
#### Identifies if a table is marked as a [date tables](https://learn.microsoft.com/power-bi/transform-model/desktop-date-tables).

```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.is_date_table(
        table_name='',
)
```

### Parameters
> **table_name** (str)
>
>> Required; Name of the table.
>
### Returns
> bool; Indicates if the table is marked as a date table.
### [is_direct_lake](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.is_direct_lake)
#### Identifies if a semantic model is in [Direct Lake](https://learn.microsoft.com/fabric/get-started/direct-lake-overview) mode.

```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.is_direct_lake()
```

### Returns
> bool; Indicates if the semantic model is in Direct Lake mode.
### [is_direct_lake_using_view](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.is_direct_lake_using_view)
#### Identifies whether a semantic model is in Direct lake mode and uses views from the lakehouse.

```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.is_direct_lake_using_view()
```

### Returns
> bool; An indicator whether a semantic model is in Direct lake mode and uses views from the lakehouse.
### [is_field_parameter](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.is_field_parameter)
#### Identifies if a table is a [field parameter](https://learn.microsoft.com/power-bi/create-reports/power-bi-field-parameters).

```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.is_field_parameter(
        table_name='',
)
```

### Parameters
> **table_name** (str)
>
>> Required; Name of the table.
>
### Returns
> bool; Indicates if the table is a field parameter.
### [is_hybrid_table](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.is_hybrid_table)
#### Identifies if a table is a [hybrid table](https://learn.microsoft.com/power-bi/connect-data/service-dataset-modes-understand#hybrid-tables).

```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.is_hybrid_table(
        table_name='',
)
```

### Parameters
> **table_name** (str)
>
>> Required; Name of the table.
>
### Returns
> bool; Indicates if the table is a hybrid table.
### [mark_as_date_table](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.mark_as_date_table)
#### Marks a table as a [date table](https://learn.microsoft.com/power-bi/transform-model/desktop-date-tables).

```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.mark_as_date_table(
        table_name='',
        column_name='',
)
```

### Parameters
> **table_name** (str)
>
>> Required; Name of the table.
>
> **column_name** (str)
>
>> Required; Name of the date column in the table.
>
### [records_per_segment](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.records_per_segment)
#### Obtains the records per segment of a partition within a semantic model.

```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.records_per_segment(
        object=tom.model.Tables["Sales"].Partitions["Sales"],
)
```

### Parameters
> **object** (TOM Object)
>
>> Required; The partition object within the semantic model.
>
### Returns
> float; Number of records per segment within the partition.
### [referenced_by](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.referenced_by)
#### Obtains the objects which reference the specified object.

```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.referenced_by(
        object=,
        dependencies=labs.get_model_calc_dependencies(dataset=tom._dataset, workspace=tom._workspace),
)
```

### Parameters
> **object** (TOM Object)
>
>> Required; The TOM object within the semantic model.
>
> **dependencies** (pandas.DataFrame)
>
>> Required; A pandas dataframe with the output of the 'get_model_calc_dependencies' function.
>
### Returns
> Microsoft.AnalysisServices.Tabular.TableCollection, Microsoft.AnalysisServices.Tabular.ColumnCollection, Microsoft.AnalysisServices.Tabular.MeasureCollection; Objects which reference the specified object.
### [remove_alternate_of](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.remove_alternate_of)
#### Removes the [alternate of](https://learn.microsoft.com/dotnet/api/microsoft.analysisservices.tabular.alternateof?view=analysisservices-dotnet) property on a column.

```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.remove_alternate_of(
        table_name='',
        column_name='',
)
```

### Parameters
> **table_name** (str)
>
>> Required; Name of the table.
>
> **column_name** (str)
>
>> Required; Name of the column.
>
### [remove_annotation](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.remove_annotation)
#### Removes an [annotation](https://learn.microsoft.com/dotnet/api/microsoft.analysisservices.annotation?view=analysisservices-dotnet) on an object within the semantic model.

```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.remove_annotation(
        object=,
        name='',
)
```

### Parameters
> **object** (TOM Object)
>
>> Required; An object (i.e. table/column/measure) within a semantic model.
>
> **name** (str)
>
>> Required; Name of the annotation.
>
### [remove_extended_property](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.remove_extended_property)
#### Removes an [extended property](https://learn.microsoft.com/dotnet/api/microsoft.analysisservices.tabular.extendedproperty?view=analysisservices-dotnet) on an object within the semantic model.

```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.remove_extended_property(
        object=,
        name='',
)
```

### Parameters
> **object** (TOM Object)
>
>> Required; An object (i.e. table/column/measure) within a semantic model.
>
> **name** (str)
>
>> Required; Name of the annotation.
>
### [remove_from_perspective](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.remove_from_perspective)
#### Removes an object from a [perspective](https://learn.microsoft.com/dotnet/api/microsoft.analysisservices.perspective?view=analysisservices-dotnet).

```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.remove_from_perspective(
        object=,
        perspective_name='',
)
```

### Parameters
> **object** (TOM Object)
>
>> Required; An object (i.e. table/column/measure) within a semantic model.
>
> **perspective_name** (str)
>
>> Required; Name of the perspective.
>
### [remove_object](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.remove_object)
#### Removes an object from a semantic model.

```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.remove_object(
        object=,
)
```

### Parameters
> **object** (TOM Object)
>
>> Required; An object (i.e. table/column/measure) within a semantic model.
>
### [remove_sort_by_column](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.remove_sort_by_column)
#### Removes the sort by column for a column in a semantic model.

```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.remove_sort_by_column(
        table_name='',
        column_name='',
)
```

### Parameters
> **table_name** (str)
>
>> Required; Name of the table.
>
> **column_name** (str)
>
>> Required; Name of the column.
>
### [remove_translation](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.remove_translation)
#### Removes an object's [translation](https://learn.microsoft.com/dotnet/api/microsoft.analysisservices.tabular.culture?view=analysisservices-dotnet) value.

```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.remove_translation(
        object=,
        language='',
)
```

### Parameters
> **object** (TOM Object)
>
>> Required; An object (i.e. table/column/measure) within a semantic model.
>
> **language** (str)
>
>> Required; The language code.
>
### [remove_vertipaq_annotations](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.remove_vertipaq_annotations)
#### Removes the annotations set using the set_vertipaq_annotations function.
```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.remove_vertipaq_annotations()
```

### [row_count](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.row_count)
#### Obtains the row count of a table or partition within a semantic model.

```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.row_count(
        object=tom.model.Tables["Sales"],
)
```

### Parameters
> **object** (TOM Object)
>
>> Required; The table/partition object within the semantic model.
>
### Returns
> int; Number of rows within the TOM object.
### [set_aggregations](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.set_aggregations)
#### Sets the [aggregations](https://learn.microsoft.com/power-bi/transform-model/aggregations-advanced) (alternate of) for all the columns in an aggregation table based on a base table.

```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.set_aggregations(
        table_name='',
        agg_table_name='',
)
```

### Parameters
> **table_name** (str)
>
>> Required; Name of the base table.
>
> **agg_table_name** (str)
>
>> Required; Name of the aggregation table.
>
### [set_alternate_of](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.set_alternate_of)
#### Sets the [alternate of](https://learn.microsoft.com/dotnet/api/microsoft.analysisservices.tabular.alternateof?view=analysisservices-dotnet) property on a column.

```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.set_alternate_of(
        table_name='',
        column_name='',
        summarization_type='',
        base_table='',
        base_column=None, # This parameter is optional
)
```

### Parameters
> **table_name** (str)
>
>> Required; Name of the table.
>
> **column_name** (str)
>
>> Required; Name of the column.
>
> **summarization_type** (str)
>
>> Required; The summarization type for the column.
[Summarization valid values](https://learn.microsoft.com/dotnet/api/microsoft.analysisservices.tabular.alternateof.summarization?view=analysisservices-dotnet#microsoft-analysisservices-tabular-alternateof-summarization)
>
> **base_table** (str)
>
>> Required; Name of the base table for aggregation.
>
> **base_column** (str)
>
>> Optional; Name of the base column for aggregation
>
### [set_annotation](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.set_annotation)
#### Sets an [annotation](https://learn.microsoft.com/dotnet/api/microsoft.analysisservices.annotation?view=analysisservices-dotnet) on an object within the semantic model.

```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.set_annotation(
        object=,
        name='',
        value='',
)
```

### Parameters
> **object** (TOM Object)
>
>> Required; An object (i.e. table/column/measure) within a semantic model.
>
> **name** (str)
>
>> Required; Name of the annotation.
>
> **value** (str)
>
>> Required; Value of the annotation.
>
### [set_data_coverage_definition](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.set_data_coverage_definition)
#### Sets the [data coverage definition](https://learn.microsoft.com/analysis-services/tom/table-partitions?view=asallproducts-allversions) for a partition.

```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.set_data_coverage_definition(
        table_name='',
        partition_name='',
        expression='',
)
```

### Parameters
> **table_name** (str)
>
>> Required; Name of the table.
>
> **partition_name** (str)
>
>> Required; Name of the partition.
>
> **expression** (str)
>
>> Required; DAX expression containing the logic for the data coverage definition.
>
### [set_data_type](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.set_data_type)
#### Sets the [data type](https://learn.microsoft.com/dotnet/api/microsoft.analysisservices.datatype?view=analysisservices-dotnet) for a column.

```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.set_data_type(
        table_name='',
        column_name='',
        value='',
)
```

### Parameters
> **table_name** (str)
>
>> Required; Name of the table.
>
> **column_name** (str)
>
>> Required; Name of the column.
>
> **value** (str)
>
>> Required; The data type.
[Data type valid values](https://learn.microsoft.com/dotnet/api/microsoft.analysisservices.datatype?view=analysisservices-dotnet)
>
### [set_direct_lake_behavior](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.set_direct_lake_behavior)
#### Sets the [Direct Lake Behavior](https://learn.microsoft.com/fabric/get-started/direct-lake-overview#fallback-behavior) property for a semantic model.

```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.set_direct_lake_behavior(
        direct_lake_behavior='',
)
```

### Parameters
> **direct_lake_behavior** (str)
>
>> Required; The DirectLakeBehavior property value.
[DirectLakeBehavior valid values](https://learn.microsoft.com/dotnet/api/microsoft.analysisservices.tabular.directlakebehavior?view=analysisservices-dotnet)
>
### [set_encoding_hint](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.set_encoding_hint)
#### Sets the [encoding hint](https://learn.microsoft.com/dotnet/api/microsoft.analysisservices.tabular.encodinghinttype?view=analysisservices-dotnet) for a column.

```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.set_encoding_hint(
        table_name='',
        column_name='',
        value='',
)
```

### Parameters
> **table_name** (str)
>
>> Required; Name of the table.
>
> **column_name** (str)
>
>> Required; Name of the column.
>
> **value** (str)
>
>> Required; Encoding hint value.
[Encoding hint valid values](https://learn.microsoft.com/dotnet/api/microsoft.analysisservices.tabular.encodinghinttype?view=analysisservices-dotnet)
>
### [set_extended_property](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.set_extended_property)
#### Sets an [extended property](https://learn.microsoft.com/dotnet/api/microsoft.analysisservices.tabular.extendedproperty?view=analysisservices-dotnet) on an object within the semantic model.

```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.set_extended_property(
        object=,
        extended_property_type='',
        name='',
        value='',
)
```

### Parameters
> **object** (TOM Object)
>
>> Required; An object (i.e. table/column/measure) within a semantic model.
>
> **extended_property_type** (str)
>
>> Required; The extended property type.
[Extended property valid values](https://learn.microsoft.com/dotnet/api/microsoft.analysisservices.tabular.extendedpropertytype?view=analysisservices-dotnet)
>
> **name** (str)
>
>> Required; Name of the extended property.
>
> **value** (str)
>
>> Required; Value of the extended property.
>
### [set_is_available_in_mdx](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.set_is_available_in_mdx)
#### Sets the [IsAvailableInMDX](https://learn.microsoft.com/dotnet/api/microsoft.analysisservices.tabular.column.isavailableinmdx?view=analysisservices-dotnet#microsoft-analysisservices-tabular-column-isavailableinmdx) property on a column.

```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.set_is_available_in_mdx(
        table_name='',
        column_name='',
        value=False, # This parameter is optional
)
```

### Parameters
> **table_name** (str)
>
>> Required; Name of the table.
>
> **column_name** (str)
>
>> Required; Name of the column.
>
> **value** (bool, default=False)
>
>> Optional; The IsAvailableInMdx property value.
>
### [set_kpi](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.set_kpi)
#### Sets the properties to add/update a [KPI](https://learn.microsoft.com/dotnet/api/microsoft.analysisservices.kpi?view=analysisservices-dotnet) for a measure.

```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.set_kpi(
        measure_name='',
        target='',
        lower_bound='',
        upper_bound='',
        lower_mid_bound=None, # This parameter is optional
        upper_mid_bound=None, # This parameter is optional
        status_type=None, # This parameter is optional
        status_graphic=None, # This parameter is optional
)
```

### Parameters
> **measure_name** (str)
>
>> Required; Name of the measure.
>
> **target** (str, int, float)
>
>> Required; The target for the KPI. This can either be a number or the name of a different measure in the semantic model.
>
> **lower_bound** (float)
>
>> Required; The lower bound for the KPI.
>
> **upper_bound** (float)
>
>> Required; The upper bound for the KPI.
>
> **lower_mid_bound** (float, default=None)
>
>> Optional; The lower-mid bound for the KPI. Set this if status_type is 'Centered' or 'CenteredReversed'.
>
> **upper_mid_bound** (float, default=None)
>
>> Optional; The upper-mid bound for the KPI. Set this if status_type is 'Centered' or 'CenteredReversed'.
>
> **status_type** (str, default=None)
>
>> Optional; The status type of the KPI. Options: 'Linear', 'LinearReversed', 'Centered', 'CenteredReversed'.
Defaults to None which resolvs to 'Linear'.
>
> **status_graphic** (str, default=None)
>
>> Optional; The status graphic for the KPI.
Defaults to 'Three Circles Colored'.
>
### [set_ols](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.set_ols)
#### Sets the object level security permissions for a column within a role.

```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.set_ols(
        role_name='',
        table_name='',
        column_name='',
        permission='',
)
```

### Parameters
> **role_name** (str)
>
>> Required; Name of the role.
>
> **table_name** (str)
>
>> Required; Name of the table.
>
> **column_name** (str)
>
>> Required; Name of the column.
>
> **permission** (str)
>
>> Required; The object level security permission for the column.
[Permission valid values](https://learn.microsoft.com/dotnet/api/microsoft.analysisservices.tabular.metadatapermission?view=analysisservices-dotnet)
>
### [set_rls](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.set_rls)
#### Sets the row level security permissions for a table within a role.

```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.set_rls(
        role_name='',
        table_name='',
        filter_expression='',
)
```

### Parameters
> **role_name** (str)
>
>> Required; Name of the role.
>
> **table_name** (str)
>
>> Required; Name of the table.
>
> **filter_expression** (str)
>
>> Required; The DAX expression containing the row level security filter expression logic.
>
### [set_sort_by_column](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.set_sort_by_column)
#### Sets the sort by column for a column in a semantic model.

```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.set_sort_by_column(
        table_name='',
        column_name='',
        sort_by_column='',
)
```

### Parameters
> **table_name** (str)
>
>> Required; Name of the table.
>
> **column_name** (str)
>
>> Required; Name of the column.
>
> **sort_by_column** (str)
>
>> Required; Name of the column to use for sorting. Must be of integer (Int64) data type.
>
### [set_summarize_by](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.set_summarize_by)
#### Sets the [SummarizeBy](https://learn.microsoft.com/dotnet/api/microsoft.analysisservices.tabular.column.summarizeby?view=analysisservices-dotnet#microsoft-analysisservices-tabular-column-summarizeby) property on a column.

```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.set_summarize_by(
        table_name='',
        column_name='',
        value='Default', # This parameter is optional
)
```

### Parameters
> **table_name** (str)
>
>> Required; Name of the table.
>
> **column_name** (str)
>
>> Required; Name of the column.
>
> **value** (bool, default=None)
>
>> Optional; The SummarizeBy property value.
Defaults to none which resolves to 'Default'.
[Aggregate valid values](https://learn.microsoft.com/dotnet/api/microsoft.analysisservices.tabular.aggregatefunction?view=analysisservices-dotnet)
>
### [set_translation](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.set_translation)
#### Sets a [translation](https://learn.microsoft.com/dotnet/api/microsoft.analysisservices.tabular.culture?view=analysisservices-dotnet) value for an object's property.

```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.set_translation(
        object=tom.model.Tables["Sales"],
        language='',
        property='',
        value='',
)
```

### Parameters
> **object** (TOM Object)
>
>> Required; An object (i.e. table/column/measure) within a semantic model.
>
> **language** (str)
>
>> Required; The language code.
>
> **property** (str)
>
>> Required; The property to set. Options: 'Name', 'Description', 'Display Folder'.
>
> **value** (str)
>
>> Required; The transation value.
>
### [set_vertipaq_annotations](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.set_vertipaq_annotations)
#### Saves Vertipaq Analyzer statistics as annotations on objects in the semantic model.
```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.set_vertipaq_annotations()
```

### [show_incremental_refresh_policy](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.show_incremental_refresh_policy)
#### Prints the [incremental refresh](https://learn.microsoft.com/power-bi/connect-data/incremental-refresh-overview) policy for a table.

```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.show_incremental_refresh_policy(
        table_name='',
)
```

### Parameters
> **table_name** (str)
>
>> Required; Name of the table.
>
### [total_size](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.total_size)
#### Obtains the data size of a table/column within a semantic model.

```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.total_size(
        object=tom.model.Tables["Sales"],
)
```

### Parameters
> **object** (TOM Object)
>
>> Required; The table/column object within the semantic model.
>
### Returns
> int; Total size of the TOM table/column.
### [unqualified_columns](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.unqualified_columns)
#### Obtains all unqualified column references for a given object.

```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.unqualified_columns(
        object=,
        dependencies=labs.get_model_calc_dependencies(dataset=tom._dataset, workspace=tom._workspace),
)
```

### Parameters
> **object** (TOM Object)
>
>> Required; The TOM object within the semantic model.
>
> **dependencies** (pandas.DataFrame)
>
>> Required; A pandas dataframe with the output of the 'get_model_calc_dependencies' function.
>
### Returns
> Microsoft.AnalysisServices.Tabular.ColumnCollection; All unqualified column references for a given object.
### [update_calculation_item](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.update_calculation_item)
#### Updates a calculation item within a semantic model.

```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.update_calculation_item(
        table_name='',
        calculation_item_name='',
        expression=None, # This parameter is optional
        ordinal=None, # This parameter is optional
        description=None, # This parameter is optional
        format_string_expression=None, # This parameter is optional
)
```

### Parameters
> **table_name** (str)
>
>> Required; Name of the calculation group (table).
>
> **calculation_item_name** (str)
>
>> Required; Name of the calculation item.
>
> **expression** (str, default=None)
>
>> Optional; The DAX expression of the calculation item.
Defaults to None which keeps the existing setting.
>
> **ordinal** (int, default=None)
>
>> Optional; The ordinal of the calculation item.
Defaults to None which keeps the existing setting.
>
> **description** (str, default=None)
>
>> Optional; The description of the role.
Defaults to None which keeps the existing setting.
>
> **format_string_expression** (str, default=None)
>
>> Optional; The format string expression for the calculation item.
Defaults to None which keeps the existing setting.
>
### [update_column](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.update_column)
#### Updates a column within a semantic model.

```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.update_column(
        table_name='',
        column_name='',
        source_column=None, # This parameter is optional
        data_type=None, # This parameter is optional
        expression=None, # This parameter is optional
        format_string=None, # This parameter is optional
        hidden=None, # This parameter is optional
        description=None, # This parameter is optional
        display_folder=None, # This parameter is optional
        data_category=None, # This parameter is optional
        key=None, # This parameter is optional
        summarize_by=None, # This parameter is optional
)
```

### Parameters
> **table_name** (str)
>
>> Required; Name of the table in which the column exists.
>
> **column_name** (str)
>
>> Required; Name of the column.
>
> **source_column** (str, default=None)
>
>> Optional; The source column for the column (for data columns only).
Defaults to None which keeps the existing setting.
>
> **data_type** (str, default=None)
>
>> Optional; The data type of the column.
Defaults to None which keeps the existing setting.
>
> **expression** (str, default=None)
>
>> Optional; The DAX expression of the column (for calculated columns only).
Defaults to None which keeps the existing setting.
>
> **format_string** (str, default=None)
>
>> Optional; Format string of the column.
Defaults to None which keeps the existing setting.
>
> **hidden** (bool, default=None)
>
>> Optional; Whether the column will be hidden or visible.
Defaults to None which keeps the existing setting.
>
> **description** (str, default=None)
>
>> Optional; A description of the column.
Defaults to None which keeps the existing setting.
>
> **display_folder** (str, default=None)
>
>> Optional; The display folder in which the column will reside.
Defaults to None which keeps the existing setting.
>
> **data_category** (str, default=None)
>
>> Optional; The data category of the column.
Defaults to None which keeps the existing setting.
>
> **key** (bool, default=False)
>
>> Optional; Marks the column as the primary key of the table.
Defaults to None which keeps the existing setting.
>
> **summarize_by** (str, default=None)
>
>> Optional; Sets the value for the Summarize By property of the column.
Defaults to None which keeps the existing setting.
>
### [update_incremental_refresh_policy](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.update_incremental_refresh_policy)
#### Updates the [incremental refresh](https://learn.microsoft.com/power-bi/connect-data/incremental-refresh-overview) policy for a table within a semantic model.

```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.update_incremental_refresh_policy(
        table_name='',
        incremental_granularity='',
        incremental_periods='',
        rolling_window_granularity='',
        rolling_window_periods='',
        only_refresh_complete_days=False, # This parameter is optional
        detect_data_changes_column=None, # This parameter is optional
)
```

### Parameters
> **table_name** (str)
>
>> Required; Name of the table.
>
> **incremental_granularity** (str)
>
>> Required; Granularity of the (most recent) incremental refresh range.
>
> **incremental_periods** (int)
>
>> Required; Number of periods for the incremental refresh range.
>
> **rolling_window_granularity** (str)
>
>> Required; Target granularity of the rolling window for the whole semantic model.
>
> **rolling_window_periods** (int)
>
>> Required; Number of periods for the rolling window for the whole semantic model.
>
> **only_refresh_complete_days** (bool, default=False)
>
>> Optional; Lag or leading periods from Now() to the rolling window head.
>
> **detect_data_changes_column** (str, default=None)
>
>> Optional; The column to use for detecting data changes.
Defaults to None which resolves to not detecting data changes.
>
### [update_m_partition](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.update_m_partition)
#### Updates an M partition for a table within a semantic model.

```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.update_m_partition(
        table_name='',
        partition_name='',
        expression=None, # This parameter is optional
        mode=None, # This parameter is optional
        description=None, # This parameter is optional
)
```

### Parameters
> **table_name** (str)
>
>> Required; Name of the table.
>
> **partition_name** (str)
>
>> Required; Name of the partition.
>
> **expression** (str, default=None)
>
>> Optional; The [M expression](https://learn.microsoft.com/dotnet/api/microsoft.analysisservices.tabular.mpartitionsource.expression?view=analysisservices-dotnet) containing the logic for the partition.
Defaults to None which keeps the existing setting.
>
> **mode** (str, default=None)
>
>> Optional; The query [mode](https://learn.microsoft.com/dotnet/api/microsoft.analysisservices.tabular.modetype?view=analysisservices-dotnet) of the partition.
Defaults to None which keeps the existing setting.
>
> **description** (str, default=None)
>
>> Optional; The description of the partition.
Defaults to None which keeps the existing setting.
>
### [update_measure](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.update_measure)
#### Updates a measure within a semantic model.

```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.update_measure(
        measure_name='',
        expression=None, # This parameter is optional
        format_string=None, # This parameter is optional
        hidden=None, # This parameter is optional
        description=None, # This parameter is optional
        display_folder=None, # This parameter is optional
        format_string_expression=None, # This parameter is optional
)
```

### Parameters
> **measure_name** (str)
>
>> Required; Name of the measure.
>
> **expression** (str, default=None)
>
>> Optional; DAX expression of the measure.
Defaults to None which keeps the existing setting.
>
> **format_string** (str, default=None)
>
>> Optional; Format string of the measure.
Defaults to None which keeps the existing setting.
>
> **hidden** (bool, default=None)
>
>> Optional; Whether the measure will be hidden or visible.
Defaults to None which keeps the existing setting.
>
> **description** (str, default=None)
>
>> Optional; A description of the measure.
Defaults to None which keeps the existing setting.
>
> **display_folder** (str, default=None)
>
>> Optional; The display folder in which the measure will reside.
Defaults to None which keeps the existing setting.
>
> **format_string_expression** (str, default=None)
>
>> Optional; The format string expression for the calculation item.
Defaults to None which keeps the existing setting.
>
### [update_role](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.update_role)
#### Updates a role within a semantic model.

```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.update_role(
        role_name='',
        model_permission=None, # This parameter is optional
        description=None, # This parameter is optional
)
```

### Parameters
> **role_name** (str)
>
>> Required; Name of the role.
>
> **model_permission** (str, default=None)
>
>> Optional; The model permission for the role.
Defaults to None which keeps the existing setting.
>
> **description** (str, default=None)
>
>> Optional; The description of the role.
Defaults to None which keeps the existing setting.
>
### [used_in_calc_item](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.used_in_calc_item)
#### Identifies the ... which reference a given object.

```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.used_in_calc_item(
        object=,
        dependencies=labs.get_model_calc_dependencies(dataset=tom._dataset, workspace=tom._workspace),
)
```

### Parameters
> **object** (TOM Object)
>
>> Required; An object (i.e. table/column) within a semantic model.
>
> **dependencies** (pandas.DataFrame)
>
>> Required; A pandas dataframe with the output of the 'get_model_calc_dependencies' function.
>
### Returns
> Microsoft.AnalysisServices.Tabular.TableCollection, Microsoft.AnalysisServices.Tabular.ColumnCollection, Microsoft.AnalysisServices.Tabular.MeasureCollection; None
### [used_in_data_coverage_definition](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.used_in_data_coverage_definition)
#### Identifies the ... which reference a given object.

```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.used_in_data_coverage_definition(
        object=,
        dependencies=labs.get_model_calc_dependencies(dataset=tom._dataset, workspace=tom._workspace),
)
```

### Parameters
> **object** (TOM Object)
>
>> Required; An object (i.e. table/column) within a semantic model.
>
> **dependencies** (pandas.DataFrame)
>
>> Required; A pandas dataframe with the output of the 'get_model_calc_dependencies' function.
>
### Returns
> Microsoft.AnalysisServices.Tabular.TableCollection, Microsoft.AnalysisServices.Tabular.ColumnCollection, Microsoft.AnalysisServices.Tabular.MeasureCollection; None
### [used_in_hierarchies](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.used_in_hierarchies)
#### Shows all [hierarchies](https://learn.microsoft.com/dotnet/api/microsoft.analysisservices.hierarchy?view=analysisservices-dotnet) in which a column is used.

```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.used_in_hierarchies(
        column=tom.model.Tables["Geography"].Columns["GeographyKey"],
)
```

### Parameters
> **object** (TOM Object)
>
>> Required; An column object within a semantic model.
>
### Returns
> Microsoft.AnalysisServices.Tabular.HierarchyCollection; All hierarchies in which the column is used.
### [used_in_levels](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.used_in_levels)
#### Shows all [levels](https://learn.microsoft.com/dotnet/api/microsoft.analysisservices.level?view=analysisservices-dotnet) in which a column is used.

```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.used_in_levels(
        column=tom.model.Tables["Geography"].Columns["GeographyKey"],
)
```

### Parameters
> **object** (TOM Object)
>
>> Required; An column object within a semantic model.
>
### Returns
> Microsoft.AnalysisServices.Tabular.LevelCollection; All levels in which the column is used.
### [used_in_relationships](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.used_in_relationships)
#### Shows all [relationships](https://learn.microsoft.com/dotnet/api/microsoft.analysisservices.tabular.singlecolumnrelationship?view=analysisservices-dotnet) in which a table/column is used.

```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.used_in_relationships(
        object=tom.model.Tables["Sales"],
)
```

### Parameters
> **object** (TOM Object)
>
>> Required; An object (i.e. table/column) within a semantic model.
>
### Returns
> Microsoft.AnalysisServices.Tabular.RelationshipCollection; All relationships in which the table/column is used.
### [used_in_rls](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.used_in_rls)
#### Identifies the row level security [filter expressions](https://learn.microsoft.com/dotnet/api/microsoft.analysisservices.tabular.tablepermission.filterexpression?view=analysisservices-dotnet#microsoft-analysisservices-tabular-tablepermission-filterexpression) which reference a given object.

```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.used_in_rls(
        object=tom.model.Tables["Sales"],
        dependencies=labs.get_model_calc_dependencies(dataset=tom._dataset, workspace=tom._workspace),
)
```

### Parameters
> **object** (TOM Object)
>
>> Required; An object (i.e. table/column) within a semantic model.
>
> **dependencies** (pandas.DataFrame)
>
>> Required; A pandas dataframe with the output of the 'get_model_calc_dependencies' function.
>
### Returns
> Microsoft.AnalysisServices.Tabular.TableCollection, Microsoft.AnalysisServices.Tabular.ColumnCollection, Microsoft.AnalysisServices.Tabular.MeasureCollection; None
### [used_in_sort_by](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.used_in_sort_by)
#### Shows all columns in which a column is used for sorting.

```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.used_in_sort_by(
        column=tom.model.Tables["Geography"].Columns["GeographyKey"],
)
```

### Parameters
> **object** (TOM Object)
>
>> Required; An column object within a semantic model.
>
### Returns
> Microsoft.AnalysisServices.Tabular.ColumnCollection; All columns in which the column is used for sorting.
### [used_size](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.used_size)
#### Obtains the used size of a hierarchy or relationship within a semantic model.

```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.used_size(
        object=tom.model.Tables["Geography"].Hierarchies["Geo Hierarchy"],
)
```

### Parameters
> **object** (TOM Object)
>
>> Required; The hierarhcy/relationship object within the semantic model.
>
### Returns
> int; Used size of the TOM object.