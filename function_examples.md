## Function Examples

### [add_user_to_workspace](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.add_user_to_workspace)
```python
import sempy_labs as labs
labs.add_user_to_workspace(
    email_address='hello@goodbye.com',
    role_name='',
    principal_type='User', # This parameter is optional
    workspace=None, # This parameter is optional
)
```

### [assign_workspace_to_capacity](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.assign_workspace_to_capacity)
```python
import sempy_labs as labs
labs.assign_workspace_to_capacity(
    capacity_name='',
    workspace=None, # This parameter is optional
)
```

### [assign_workspace_to_dataflow_storage](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.assign_workspace_to_dataflow_storage)
```python
import sempy_labs as labs
labs.assign_workspace_to_dataflow_storage(
    dataflow_storage_account='',
    workspace=None, # This parameter is optional
)
```

### [backup_semantic_model](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.backup_semantic_model)
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

### [cancel_dataset_refresh](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.cancel_dataset_refresh)
```python
import sempy_labs as labs
labs.cancel_dataset_refresh(
    dataset='AdvWorks',
    request_id=None, # This parameter is optional
    workspace=None, # This parameter is optional
)
```

### [clear_cache](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.clear_cache)
```python
import sempy_labs as labs
labs.clear_cache(
    dataset='AdvWorks',
    workspace=None, # This parameter is optional
)
```

### [commit_to_git](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.commit_to_git)
```python
import sempy_labs as labs
labs.commit_to_git(
    comment='',
    item_ids=None, # This parameter is optional
    workspace=None, # This parameter is optional
)
```

### [connect_workspace_to_git](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.connect_workspace_to_git)
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

### [copy_semantic_model_backup_file](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.copy_semantic_model_backup_file)
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

### [create_abfss_path](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.create_abfss_path)
```python
import sempy_labs as labs
labs.create_abfss_path(
    lakehouse_id='',
    lakehouse_workspace_id='',
    delta_table_name='',
)
```

### [create_blank_semantic_model](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.create_blank_semantic_model)
```python
import sempy_labs as labs
labs.create_blank_semantic_model(
    dataset='AdvWorks',
    compatibility_level='1605', # This parameter is optional
    workspace=None, # This parameter is optional
)
```

### [create_custom_pool](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.create_custom_pool)
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

### [create_model_bpa_semantic_model](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.create_model_bpa_semantic_model)
```python
import sempy_labs as labs
labs.create_model_bpa_semantic_model(
    dataset='ModelBPA', # This parameter is optional
    lakehouse=None, # This parameter is optional
    lakehouse_workspace=None, # This parameter is optional
)
```

### [create_relationship_name](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.create_relationship_name)
```python
import sempy_labs as labs
labs.create_relationship_name(
    from_table='',
    from_column='',
    to_table='',
    to_column='',
)
```

### [create_semantic_model_from_bim](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.create_semantic_model_from_bim)
```python
import sempy_labs as labs
labs.create_semantic_model_from_bim(
    dataset='AdvWorks',
    bim_file='',
    workspace=None, # This parameter is optional
)
```

### [create_warehouse](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.create_warehouse)
```python
import sempy_labs as labs
labs.create_warehouse(
    warehouse='',
    description=None, # This parameter is optional
    workspace=None, # This parameter is optional
)
```

### [delete_custom_pool](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.delete_custom_pool)
```python
import sempy_labs as labs
labs.delete_custom_pool(
    pool_name='',
    workspace=None, # This parameter is optional
)
```

### [delete_user_from_workspace](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.delete_user_from_workspace)
```python
import sempy_labs as labs
labs.delete_user_from_workspace(
    email_address='hello@goodbye.com',
    workspace=None, # This parameter is optional
)
```

### [deploy_semantic_model](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.deploy_semantic_model)
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

### [deprovision_workspace_identity](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.deprovision_workspace_identity)
```python
import sempy_labs as labs
labs.deprovision_workspace_identity(
    workspace=None, # This parameter is optional
)
```

### [disable_qso](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.disable_qso)
```python
import sempy_labs as labs
labs.disable_qso(
    dataset='AdvWorks',
    workspace=None, # This parameter is optional
)
```

### [disconnect_workspace_from_git](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.disconnect_workspace_from_git)
```python
import sempy_labs as labs
labs.disconnect_workspace_from_git(
    workspace=None, # This parameter is optional
)
```

### [evaluate_dax_impersonation](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.evaluate_dax_impersonation)
```python
import sempy_labs as labs
labs.evaluate_dax_impersonation(
    dataset='AdvWorks',
    dax_query='EVALUATE SUMMARIZECOLUMNS("MyMeasure", 1)',
    user_name=None, # This parameter is optional
    workspace=None, # This parameter is optional
)
```

### [export_model_to_onelake](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.export_model_to_onelake)
```python
import sempy_labs as labs
labs.export_model_to_onelake(
    dataset='AdvWorks',
    workspace=None, # This parameter is optional
    destination_lakehouse=None, # This parameter is optional
    destination_workspace=None, # This parameter is optional
)
```

### [format_dax_object_name](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.format_dax_object_name)
```python
import sempy_labs as labs
labs.format_dax_object_name(
    table='',
    column=tom.model.Tables["Geography"].Columns["GeographyKey"],
)
```

### [generate_embedded_filter](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.generate_embedded_filter)
```python
import sempy_labs as labs
labs.generate_embedded_filter(
    filter='',
)
```

### [get_capacity_id](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.get_capacity_id)
```python
import sempy_labs as labs
labs.get_capacity_id(
    workspace=None, # This parameter is optional
)
```

### [get_capacity_name](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.get_capacity_name)
```python
import sempy_labs as labs
labs.get_capacity_name(
    workspace=None, # This parameter is optional
)
```

### [get_direct_lake_sql_endpoint](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.get_direct_lake_sql_endpoint)
```python
import sempy_labs as labs
labs.get_direct_lake_sql_endpoint(
    dataset='AdvWorks',
    workspace=None, # This parameter is optional
)
```

### [get_git_connection](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.get_git_connection)
```python
import sempy_labs as labs
labs.get_git_connection(
    workspace=None, # This parameter is optional
)
```

### [get_git_status](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.get_git_status)
```python
import sempy_labs as labs
labs.get_git_status(
    workspace=None, # This parameter is optional
)
```

### [get_measure_dependencies](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.get_measure_dependencies)
```python
import sempy_labs as labs
labs.get_measure_dependencies(
    dataset='AdvWorks',
    workspace=None, # This parameter is optional
)
```

### [get_model_calc_dependencies](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.get_model_calc_dependencies)
```python
import sempy_labs as labs
labs.get_model_calc_dependencies(
    dataset='AdvWorks',
    workspace=None, # This parameter is optional
)
```

### [get_notebook_definition](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.get_notebook_definition)
```python
import sempy_labs as labs
labs.get_notebook_definition(
    notebook_name='',
    workspace=None, # This parameter is optional
    decode=True, # This parameter is optional
)
```

### [get_object_level_security](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.get_object_level_security)
```python
import sempy_labs as labs
labs.get_object_level_security(
    dataset='AdvWorks',
    workspace=None, # This parameter is optional
)
```

### [get_semantic_model_bim](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.get_semantic_model_bim)
```python
import sempy_labs as labs
labs.get_semantic_model_bim(
    dataset='AdvWorks',
    workspace=None, # This parameter is optional
    save_to_file_name=None, # This parameter is optional
    lakehouse_workspace=None, # This parameter is optional
)
```

### [get_spark_settings](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.get_spark_settings)
```python
import sempy_labs as labs
labs.get_spark_settings(
    workspace=None, # This parameter is optional
)
```

### [import_notebook_from_web](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.import_notebook_from_web)
```python
import sempy_labs as labs
labs.import_notebook_from_web(
    notebook_name='',
    url='',
    description=None, # This parameter is optional
    workspace=None, # This parameter is optional
)
```

### [import_vertipaq_analyzer](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.import_vertipaq_analyzer)
```python
import sempy_labs as labs
labs.import_vertipaq_analyzer(
    folder_path='',
    file_name='',
)
```

### [initialize_git_connection](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.initialize_git_connection)
```python
import sempy_labs as labs
labs.initialize_git_connection(
    workspace=None, # This parameter is optional
)
```

### [is_default_semantic_model](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.is_default_semantic_model)
```python
import sempy_labs as labs
labs.is_default_semantic_model(
    dataset='AdvWorks',
    workspace=None, # This parameter is optional
)
```

### [list_capacities](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.list_capacities)
```python
import sempy_labs as labs
labs.list_capacities()
```

### [list_custom_pools](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.list_custom_pools)
```python
import sempy_labs as labs
labs.list_custom_pools(
    workspace=None, # This parameter is optional
)
```

### [list_dashboards](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.list_dashboards)
```python
import sempy_labs as labs
labs.list_dashboards(
    workspace=None, # This parameter is optional
)
```

### [list_dataflow_storage_accounts](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.list_dataflow_storage_accounts)
```python
import sempy_labs as labs
labs.list_dataflow_storage_accounts()
```

### [list_dataflows](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.list_dataflows)
```python
import sempy_labs as labs
labs.list_dataflows(
    workspace=None, # This parameter is optional
)
```

### [list_deployment_pipeline_stage_items](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.list_deployment_pipeline_stage_items)
```python
import sempy_labs as labs
labs.list_deployment_pipeline_stage_items(
    deployment_pipeline='',
    stage_name='',
)
```

### [list_deployment_pipeline_stages](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.list_deployment_pipeline_stages)
```python
import sempy_labs as labs
labs.list_deployment_pipeline_stages(
    deployment_pipeline='',
)
```

### [list_deployment_pipelines](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.list_deployment_pipelines)
```python
import sempy_labs as labs
labs.list_deployment_pipelines()
```

### [list_lakehouses](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.list_lakehouses)
```python
import sempy_labs as labs
labs.list_lakehouses(
    workspace=None, # This parameter is optional
)
```

### [list_qso_settings](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.list_qso_settings)
```python
import sempy_labs as labs
labs.list_qso_settings(
    dataset=None, # This parameter is optional
    workspace=None, # This parameter is optional
)
```

### [list_reports_using_semantic_model](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.list_reports_using_semantic_model)
```python
import sempy_labs as labs
labs.list_reports_using_semantic_model(
    dataset='AdvWorks',
    workspace=None, # This parameter is optional
)
```

### [list_semantic_model_objects](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.list_semantic_model_objects)
```python
import sempy_labs as labs
labs.list_semantic_model_objects(
    dataset='AdvWorks',
    workspace=None, # This parameter is optional
)
```

### [list_shortcuts](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.list_shortcuts)
```python
import sempy_labs as labs
labs.list_shortcuts(
    lakehouse=None, # This parameter is optional
    workspace=None, # This parameter is optional
)
```

### [list_warehouses](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.list_warehouses)
```python
import sempy_labs as labs
labs.list_warehouses(
    workspace=None, # This parameter is optional
)
```

### [list_workspace_role_assignments](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.list_workspace_role_assignments)
```python
import sempy_labs as labs
labs.list_workspace_role_assignments(
    workspace=None, # This parameter is optional
)
```

### [list_workspace_users](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.list_workspace_users)
```python
import sempy_labs as labs
labs.list_workspace_users(
    workspace=None, # This parameter is optional
)
```

### [measure_dependency_tree](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.measure_dependency_tree)
```python
import sempy_labs as labs
labs.measure_dependency_tree(
    dataset='AdvWorks',
    measure_name='',
    workspace=None, # This parameter is optional
)
```

### [model_bpa_rules](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.model_bpa_rules)
```python
import sempy_labs as labs
labs.model_bpa_rules(
    dataset='AdvWorks',
    workspace=None, # This parameter is optional
    dependencies=None, # This parameter is optional
)
```

### [provision_workspace_identity](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.provision_workspace_identity)
```python
import sempy_labs as labs
labs.provision_workspace_identity(
    workspace=None, # This parameter is optional
)
```

### [qso_sync](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.qso_sync)
```python
import sempy_labs as labs
labs.qso_sync(
    dataset='AdvWorks',
    workspace=None, # This parameter is optional
)
```

### [qso_sync_status](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.qso_sync_status)
```python
import sempy_labs as labs
labs.qso_sync_status(
    dataset='AdvWorks',
    workspace=None, # This parameter is optional
)
```

### [refresh_semantic_model](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.refresh_semantic_model)
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

### [resolve_capacity_name](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.resolve_capacity_name)
```python
import sempy_labs as labs
labs.resolve_capacity_name(
    capacity_id=None, # This parameter is optional
)
```

### [resolve_dataset_id](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.resolve_dataset_id)
```python
import sempy_labs as labs
labs.resolve_dataset_id(
    dataset='AdvWorks',
    workspace=None, # This parameter is optional
)
```

### [resolve_dataset_name](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.resolve_dataset_name)
```python
import sempy_labs as labs
labs.resolve_dataset_name(
    dataset_id='',
    workspace=None, # This parameter is optional
)
```

### [resolve_item_type](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.resolve_item_type)
```python
import sempy_labs as labs
labs.resolve_item_type(
    item_id='',
    workspace=None, # This parameter is optional
)
```

### [resolve_lakehouse_id](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.resolve_lakehouse_id)
```python
import sempy_labs as labs
labs.resolve_lakehouse_id(
    lakehouse='',
    workspace=None, # This parameter is optional
)
```

### [resolve_lakehouse_name](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.resolve_lakehouse_name)
```python
import sempy_labs as labs
labs.resolve_lakehouse_name(
    lakehouse_id=None, # This parameter is optional
    workspace=None, # This parameter is optional
)
```

### [resolve_report_id](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.resolve_report_id)
```python
import sempy_labs as labs
labs.resolve_report_id(
    report='',
    workspace=None, # This parameter is optional
)
```

### [resolve_report_name](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.resolve_report_name)
```python
import sempy_labs as labs
labs.resolve_report_name(
    report_id='',
    workspace=None, # This parameter is optional
)
```

### [resolve_workspace_capacity](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.resolve_workspace_capacity)
```python
import sempy_labs as labs
labs.resolve_workspace_capacity(
    workspace=None, # This parameter is optional
)
```

### [restore_semantic_model](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.restore_semantic_model)
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

### [run_model_bpa](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.run_model_bpa)
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

### [run_model_bpa_bulk](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.run_model_bpa_bulk)
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

### [save_as_delta_table](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.save_as_delta_table)
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

### [set_qso](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.set_qso)
```python
import sempy_labs as labs
labs.set_qso(
    dataset='AdvWorks',
    auto_sync=True, # This parameter is optional
    max_read_only_replicas='-1', # This parameter is optional
    workspace=None, # This parameter is optional
)
```

### [set_semantic_model_storage_format](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.set_semantic_model_storage_format)
```python
import sempy_labs as labs
labs.set_semantic_model_storage_format(
    dataset='AdvWorks',
    storage_format='',
    workspace=None, # This parameter is optional
)
```

### [set_workspace_default_storage_format](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.set_workspace_default_storage_format)
```python
import sempy_labs as labs
labs.set_workspace_default_storage_format(
    storage_format='',
    workspace=None, # This parameter is optional
)
```

### [translate_semantic_model](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.translate_semantic_model)
```python
import sempy_labs as labs
labs.translate_semantic_model(
    dataset='AdvWorks',
    languages=['it-IT', 'zh-CN'],
    exclude_characters=None, # This parameter is optional
    workspace=None, # This parameter is optional
)
```

### [unassign_workspace_from_capacity](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.unassign_workspace_from_capacity)
```python
import sempy_labs as labs
labs.unassign_workspace_from_capacity(
    workspace=None, # This parameter is optional
)
```

### [update_custom_pool](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.update_custom_pool)
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

### [update_from_git](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.update_from_git)
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

### [update_item](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.update_item)
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

### [update_spark_settings](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.update_spark_settings)
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

### [update_workspace_user](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.update_workspace_user)
```python
import sempy_labs as labs
labs.update_workspace_user(
    email_address='hello@goodbye.com',
    role_name='',
    principal_type='User', # This parameter is optional
    workspace=None, # This parameter is optional
)
```

### [vertipaq_analyzer](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.vertipaq_analyzer)
```python
import sempy_labs as labs
labs.vertipaq_analyzer(
    dataset='AdvWorks',
    workspace=None, # This parameter is optional
    export=None, # This parameter is optional
    read_stats_from_data=False, # This parameter is optional
)
```

### [add_table_to_direct_lake_semantic_model](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.directlake.html#sempy_labs.directlake.add_table_to_direct_lake_semantic_model)
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

### [check_fallback_reason](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.directlake.html#sempy_labs.directlake.check_fallback_reason)
```python
import sempy_labs as labs
import sempy_labs.directlake as directlake
directlake.check_fallback_reason(
    dataset='AdvWorks',
    workspace=None, # This parameter is optional
)
```

### [direct_lake_schema_compare](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.directlake.html#sempy_labs.directlake.direct_lake_schema_compare)
```python
import sempy_labs as labs
import sempy_labs.directlake as directlake
directlake.direct_lake_schema_compare(
    dataset='AdvWorks',
    workspace=None, # This parameter is optional
)
```

### [direct_lake_schema_sync](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.directlake.html#sempy_labs.directlake.direct_lake_schema_sync)
```python
import sempy_labs as labs
import sempy_labs.directlake as directlake
directlake.direct_lake_schema_sync(
    dataset='AdvWorks',
    workspace=None, # This parameter is optional
    add_to_model=False, # This parameter is optional
)
```

### [generate_direct_lake_semantic_model](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.directlake.html#sempy_labs.directlake.generate_direct_lake_semantic_model)
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

### [get_direct_lake_guardrails](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.directlake.html#sempy_labs.directlake.get_direct_lake_guardrails)
```python
import sempy_labs as labs
import sempy_labs.directlake as directlake
directlake.get_direct_lake_guardrails()
```

### [get_direct_lake_lakehouse](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.directlake.html#sempy_labs.directlake.get_direct_lake_lakehouse)
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

### [get_direct_lake_source](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.directlake.html#sempy_labs.directlake.get_direct_lake_source)
```python
import sempy_labs as labs
import sempy_labs.directlake as directlake
directlake.get_direct_lake_source(
    dataset='AdvWorks',
    workspace=None, # This parameter is optional
)
```

### [get_directlake_guardrails_for_sku](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.directlake.html#sempy_labs.directlake.get_directlake_guardrails_for_sku)
```python
import sempy_labs as labs
import sempy_labs.directlake as directlake
directlake.get_directlake_guardrails_for_sku(
    sku_size='',
)
```

### [get_shared_expression](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.directlake.html#sempy_labs.directlake.get_shared_expression)
```python
import sempy_labs as labs
import sempy_labs.directlake as directlake
directlake.get_shared_expression(
    lakehouse=None, # This parameter is optional
    workspace=None, # This parameter is optional
)
```

### [get_sku_size](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.directlake.html#sempy_labs.directlake.get_sku_size)
```python
import sempy_labs as labs
import sempy_labs.directlake as directlake
directlake.get_sku_size(
    workspace=None, # This parameter is optional
)
```

### [list_direct_lake_model_calc_tables](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.directlake.html#sempy_labs.directlake.list_direct_lake_model_calc_tables)
```python
import sempy_labs as labs
import sempy_labs.directlake as directlake
directlake.list_direct_lake_model_calc_tables(
    dataset='AdvWorks',
    workspace=None, # This parameter is optional
)
```

### [show_unsupported_direct_lake_objects](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.directlake.html#sempy_labs.directlake.show_unsupported_direct_lake_objects)
```python
import sempy_labs as labs
import sempy_labs.directlake as directlake
directlake.show_unsupported_direct_lake_objects(
    dataset='AdvWorks',
    workspace=None, # This parameter is optional
)
```

### [update_direct_lake_model_lakehouse_connection](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.directlake.html#sempy_labs.directlake.update_direct_lake_model_lakehouse_connection)
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

### [update_direct_lake_partition_entity](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.directlake.html#sempy_labs.directlake.update_direct_lake_partition_entity)
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

### [warm_direct_lake_cache_isresident](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.directlake.html#sempy_labs.directlake.warm_direct_lake_cache_isresident)
```python
import sempy_labs as labs
import sempy_labs.directlake as directlake
directlake.warm_direct_lake_cache_isresident(
    dataset='AdvWorks',
    workspace=None, # This parameter is optional
)
```

### [warm_direct_lake_cache_perspective](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.directlake.html#sempy_labs.directlake.warm_direct_lake_cache_perspective)
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

### [create_shortcut_onelake](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.lakehouse.html#sempy_labs.lakehouse.create_shortcut_onelake)
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

### [delete_shortcut](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.lakehouse.html#sempy_labs.lakehouse.delete_shortcut)
```python
import sempy_labs as labs
import sempy_labs.lakehouse as lake
lake.delete_shortcut(
    shortcut_name='',
    lakehouse=None, # This parameter is optional
    workspace=None, # This parameter is optional
)
```

### [get_lakehouse_columns](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.lakehouse.html#sempy_labs.lakehouse.get_lakehouse_columns)
```python
import sempy_labs as labs
import sempy_labs.lakehouse as lake
lake.get_lakehouse_columns(
    lakehouse=None, # This parameter is optional
    workspace=None, # This parameter is optional
)
```

### [get_lakehouse_tables](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.lakehouse.html#sempy_labs.lakehouse.get_lakehouse_tables)
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

### [lakehouse_attached](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.lakehouse.html#sempy_labs.lakehouse.lakehouse_attached)
```python
import sempy_labs as labs
import sempy_labs.lakehouse as lake
lake.lakehouse_attached()
```

### [optimize_lakehouse_tables](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.lakehouse.html#sempy_labs.lakehouse.optimize_lakehouse_tables)
```python
import sempy_labs as labs
import sempy_labs.lakehouse as lake
lake.optimize_lakehouse_tables(
    tables=None, # This parameter is optional
    lakehouse=None, # This parameter is optional
    workspace=None, # This parameter is optional
)
```

### [vacuum_lakehouse_tables](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.lakehouse.html#sempy_labs.lakehouse.vacuum_lakehouse_tables)
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

### [create_pqt_file](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.migration.html#sempy_labs.migration.create_pqt_file)
```python
import sempy_labs as labs
import sempy_labs.migration as migration
migration.create_pqt_file(
    dataset='AdvWorks',
    workspace=None, # This parameter is optional
    file_name='PowerQueryTemplate', # This parameter is optional
)
```

### [migrate_calc_tables_to_lakehouse](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.migration.html#sempy_labs.migration.migrate_calc_tables_to_lakehouse)
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

### [migrate_calc_tables_to_semantic_model](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.migration.html#sempy_labs.migration.migrate_calc_tables_to_semantic_model)
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

### [migrate_field_parameters](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.migration.html#sempy_labs.migration.migrate_field_parameters)
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

### [migrate_model_objects_to_semantic_model](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.migration.html#sempy_labs.migration.migrate_model_objects_to_semantic_model)
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

### [migrate_tables_columns_to_semantic_model](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.migration.html#sempy_labs.migration.migrate_tables_columns_to_semantic_model)
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

### [migration_validation](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.migration.html#sempy_labs.migration.migration_validation)
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

### [refresh_calc_tables](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.migration.html#sempy_labs.migration.refresh_calc_tables)
```python
import sempy_labs as labs
import sempy_labs.migration as migration
migration.refresh_calc_tables(
    dataset='AdvWorks',
    workspace=None, # This parameter is optional
)
```

### [clone_report](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.report.html#sempy_labs.report.clone_report)
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

### [create_model_bpa_report](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.report.html#sempy_labs.report.create_model_bpa_report)
```python
import sempy_labs as labs
import sempy_labs.report as rep
rep.create_model_bpa_report(
    report='ModelBPA', # This parameter is optional
    dataset='ModelBPA', # This parameter is optional
    dataset_workspace=None, # This parameter is optional
)
```

### [create_report_from_reportjson](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.report.html#sempy_labs.report.create_report_from_reportjson)
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

### [export_report](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.report.html#sempy_labs.report.export_report)
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

### [get_report_definition](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.report.html#sempy_labs.report.get_report_definition)
```python
import sempy_labs as labs
import sempy_labs.report as rep
rep.get_report_definition(
    report='',
    workspace=None, # This parameter is optional
)
```

### [get_report_json](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.report.html#sempy_labs.report.get_report_json)
```python
import sempy_labs as labs
import sempy_labs.report as rep
rep.get_report_json(
    report='',
    workspace=None, # This parameter is optional
    save_to_file_name=None, # This parameter is optional
)
```

### [launch_report](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.report.html#sempy_labs.report.launch_report)
```python
import sempy_labs as labs
import sempy_labs.report as rep
rep.launch_report(
    report='',
    workspace=None, # This parameter is optional
)
```

### [report_rebind](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.report.html#sempy_labs.report.report_rebind)
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

### [report_rebind_all](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.report.html#sempy_labs.report.report_rebind_all)
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

### [update_report_from_reportjson](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.report.html#sempy_labs.report.update_report_from_reportjson)
```python
import sempy_labs as labs
import sempy_labs.report as rep
rep.update_report_from_reportjson(
    report='',
    report_json='',
    workspace=None, # This parameter is optional
)
```

### [add_calculated_column](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.add_calculated_column)
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

### [add_calculated_table](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.add_calculated_table)
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

### [add_calculated_table_column](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.add_calculated_table_column)
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

### [add_calculation_group](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.add_calculation_group)
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

### [add_calculation_item](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.add_calculation_item)
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

### [add_data_column](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.add_data_column)
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

### [add_entity_partition](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.add_entity_partition)
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

### [add_expression](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.add_expression)
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

### [add_field_parameter](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.add_field_parameter)
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

### [add_hierarchy](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.add_hierarchy)
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

### [add_incremental_refresh_policy](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.add_incremental_refresh_policy)
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

### [add_m_partition](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.add_m_partition)
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

### [add_measure](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.add_measure)
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

### [add_perspective](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.add_perspective)
```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.add_perspective(
        perspective_name='',
)
```

### [add_relationship](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.add_relationship)
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

### [add_role](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.add_role)
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

### [add_table](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.add_table)
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

### [add_time_intelligence](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.add_time_intelligence)
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

### [add_to_perspective](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.add_to_perspective)
```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.add_to_perspective(
        object=,
        perspective_name='',
)
```

### [add_translation](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.add_translation)
```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.add_translation(
        language='',
)
```

### [all_calculated_columns](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.all_calculated_columns)
```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.all_calculated_columns()
```

### [all_calculated_tables](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.all_calculated_tables)
```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.all_calculated_tables()
```

### [all_calculation_groups](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.all_calculation_groups)
```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.all_calculation_groups()
```

### [all_calculation_items](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.all_calculation_items)
```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.all_calculation_items()
```

### [all_columns](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.all_columns)
```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.all_columns()
```

### [all_date_tables](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.all_date_tables)
```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.all_date_tables()
```

### [all_hierarchies](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.all_hierarchies)
```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.all_hierarchies()
```

### [all_hybrid_tables](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.all_hybrid_tables)
```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.all_hybrid_tables()
```

### [all_levels](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.all_levels)
```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.all_levels()
```

### [all_measures](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.all_measures)
```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.all_measures()
```

### [all_partitions](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.all_partitions)
```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.all_partitions()
```

### [all_rls](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.all_rls)
```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.all_rls()
```

### [apply_refresh_policy](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.apply_refresh_policy)
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

### [cardinality](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.cardinality)
```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.cardinality(
        column=tom.model.Tables["Geography"].Columns["GeographyKey"],
)
```

### [clear_annotations](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.clear_annotations)
```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.clear_annotations(
        object=,
)
```

### [clear_extended_properties](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.clear_extended_properties)
```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.clear_extended_properties(
        object=,
)
```

### [data_size](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.data_size)
```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.data_size(
        column=tom.model.Tables["Geography"].Columns["GeographyKey"],
)
```

### [depends_on](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.depends_on)
```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.depends_on(
        object=,
        dependencies=labs.get_model_calc_dependencies(dataset=tom._dataset, workspace=tom._workspace),
)
```

### [dictionary_size](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.dictionary_size)
```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.dictionary_size(
        column=tom.model.Tables["Geography"].Columns["GeographyKey"],
)
```

### [fully_qualified_measures](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.fully_qualified_measures)
```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.fully_qualified_measures(
        object=tom.model.Tables["Sales"].Measures["Sales Amount"],
        dependencies=labs.get_model_calc_dependencies(dataset=tom._dataset, workspace=tom._workspace),
)
```

### [get_annotation_value](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.get_annotation_value)
```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.get_annotation_value(
        object=,
        name='',
)
```

### [get_annotations](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.get_annotations)
```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.get_annotations(
        object=,
)
```

### [get_extended_properties](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.get_extended_properties)
```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.get_extended_properties(
        object=,
)
```

### [get_extended_property_value](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.get_extended_property_value)
```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.get_extended_property_value(
        object=,
        name='',
)
```

### [has_aggs](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.has_aggs)
```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.has_aggs()
```

### [has_date_table](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.has_date_table)
```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.has_date_table()
```

### [has_hybrid_table](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.has_hybrid_table)
```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.has_hybrid_table()
```

### [has_incremental_refresh_policy](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.has_incremental_refresh_policy)
```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.has_incremental_refresh_policy(
        table_name='',
)
```

### [in_perspective](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.in_perspective)
```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.in_perspective(
        object=,
        perspective_name='',
)
```

### [is_agg_table](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.is_agg_table)
```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.is_agg_table(
        table_name='',
)
```

### [is_auto_date_table](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.is_auto_date_table)
```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.is_auto_date_table(
        table_name='',
)
```

### [is_calculated_table](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.is_calculated_table)
```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.is_calculated_table(
        table_name='',
)
```

### [is_date_table](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.is_date_table)
```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.is_date_table(
        table_name='',
)
```

### [is_direct_lake](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.is_direct_lake)
```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.is_direct_lake()
```

### [is_direct_lake_using_view](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.is_direct_lake_using_view)
```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.is_direct_lake_using_view()
```

### [is_field_parameter](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.is_field_parameter)
```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.is_field_parameter(
        table_name='',
)
```

### [is_hybrid_table](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.is_hybrid_table)
```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.is_hybrid_table(
        table_name='',
)
```

### [mark_as_date_table](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.mark_as_date_table)
```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.mark_as_date_table(
        table_name='',
        column_name='',
)
```

### [records_per_segment](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.records_per_segment)
```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.records_per_segment(
        object=tom.model.Tables["Sales"].Partitions["Sales"],
)
```

### [referenced_by](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.referenced_by)
```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.referenced_by(
        object=,
        dependencies=labs.get_model_calc_dependencies(dataset=tom._dataset, workspace=tom._workspace),
)
```

### [remove_alternate_of](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.remove_alternate_of)
```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.remove_alternate_of(
        table_name='',
        column_name='',
)
```

### [remove_annotation](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.remove_annotation)
```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.remove_annotation(
        object=,
        name='',
)
```

### [remove_extended_property](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.remove_extended_property)
```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.remove_extended_property(
        object=,
        name='',
)
```

### [remove_from_perspective](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.remove_from_perspective)
```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.remove_from_perspective(
        object=,
        perspective_name='',
)
```

### [remove_object](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.remove_object)
```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.remove_object(
        object=,
)
```

### [remove_sort_by_column](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.remove_sort_by_column)
```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.remove_sort_by_column(
        table_name='',
        column_name='',
)
```

### [remove_translation](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.remove_translation)
```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.remove_translation(
        object=,
        language='',
)
```

### [remove_vertipaq_annotations](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.remove_vertipaq_annotations)
```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.remove_vertipaq_annotations()
```

### [row_count](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.row_count)
```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.row_count(
        object=tom.model.Tables["Sales"],
)
```

### [set_aggregations](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.set_aggregations)
```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.set_aggregations(
        table_name='',
        agg_table_name='',
)
```

### [set_alternate_of](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.set_alternate_of)
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

### [set_annotation](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.set_annotation)
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

### [set_data_coverage_definition](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.set_data_coverage_definition)
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

### [set_data_type](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.set_data_type)
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

### [set_direct_lake_behavior](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.set_direct_lake_behavior)
```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.set_direct_lake_behavior(
        direct_lake_behavior='',
)
```

### [set_encoding_hint](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.set_encoding_hint)
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

### [set_extended_property](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.set_extended_property)
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

### [set_is_available_in_mdx](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.set_is_available_in_mdx)
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

### [set_kpi](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.set_kpi)
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

### [set_ols](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.set_ols)
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

### [set_rls](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.set_rls)
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

### [set_sort_by_column](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.set_sort_by_column)
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

### [set_summarize_by](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.set_summarize_by)
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

### [set_translation](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.set_translation)
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

### [set_vertipaq_annotations](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.set_vertipaq_annotations)
```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.set_vertipaq_annotations()
```

### [show_incremental_refresh_policy](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.show_incremental_refresh_policy)
```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.show_incremental_refresh_policy(
        table_name='',
)
```

### [total_size](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.total_size)
```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.total_size(
        object=tom.model.Tables["Sales"],
)
```

### [unqualified_columns](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.unqualified_columns)
```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.unqualified_columns(
        object=,
        dependencies=labs.get_model_calc_dependencies(dataset=tom._dataset, workspace=tom._workspace),
)
```

### [update_calculation_item](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.update_calculation_item)
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

### [update_column](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.update_column)
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

### [update_incremental_refresh_policy](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.update_incremental_refresh_policy)
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

### [update_m_partition](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.update_m_partition)
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

### [update_measure](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.update_measure)
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

### [update_role](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.update_role)
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

### [used_in_calc_item](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.used_in_calc_item)
```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.used_in_calc_item(
        object=,
        dependencies=labs.get_model_calc_dependencies(dataset=tom._dataset, workspace=tom._workspace),
)
```

### [used_in_data_coverage_definition](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.used_in_data_coverage_definition)
```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.used_in_data_coverage_definition(
        object=,
        dependencies=labs.get_model_calc_dependencies(dataset=tom._dataset, workspace=tom._workspace),
)
```

### [used_in_hierarchies](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.used_in_hierarchies)
```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.used_in_hierarchies(
        column=tom.model.Tables["Geography"].Columns["GeographyKey"],
)
```

### [used_in_levels](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.used_in_levels)
```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.used_in_levels(
        column=tom.model.Tables["Geography"].Columns["GeographyKey"],
)
```

### [used_in_relationships](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.used_in_relationships)
```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.used_in_relationships(
        object=tom.model.Tables["Sales"],
)
```

### [used_in_rls](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.used_in_rls)
```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.used_in_rls(
        object=tom.model.Tables["Sales"],
        dependencies=labs.get_model_calc_dependencies(dataset=tom._dataset, workspace=tom._workspace),
)
```

### [used_in_sort_by](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.used_in_sort_by)
```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.used_in_sort_by(
        column=tom.model.Tables["Geography"].Columns["GeographyKey"],
)
```

### [used_size](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.used_size)
```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.used_size(
        object=tom.model.Tables["Geography"].Hierarchies["Geo Hierarchy"],
)
```
