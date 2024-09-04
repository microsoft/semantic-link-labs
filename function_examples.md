## Function Examples

### [add_user_to_workspace](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.add_user_to_workspace)
```python
import sempy_labs as labs
labs.add_user_to_workspace(
    email_address='',
    role_name='',
    principal_type=None,
    workspace=None,
)
```

### [assign_workspace_to_capacity](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.assign_workspace_to_capacity)
```python
import sempy_labs as labs
labs.assign_workspace_to_capacity(
    capacity_name='',
    workspace=None,
)
```

### [assign_workspace_to_dataflow_storage](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.assign_workspace_to_dataflow_storage)
```python
import sempy_labs as labs
labs.assign_workspace_to_dataflow_storage(
    dataflow_storage_account='',
    workspace=None,
)
```

### [backup_semantic_model](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.backup_semantic_model)
```python
import sempy_labs as labs
labs.backup_semantic_model(
    dataset='',
    file_path='',
    allow_overwrite=None,
    apply_compression=None,
    workspace=None,
)
```

### [cancel_dataset_refresh](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.cancel_dataset_refresh)
```python
import sempy_labs as labs
labs.cancel_dataset_refresh(
    dataset='',
    request_id=None,
    workspace=None,
)
```

### [clear_cache](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.clear_cache)
```python
import sempy_labs as labs
labs.clear_cache(
    dataset='',
    workspace=None,
)
```

### [commit_to_git](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.commit_to_git)
```python
import sempy_labs as labs
labs.commit_to_git(
    comment='',
    item_ids='',
    workspace=None,
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
    git_provider_type='',
    workspace=None,
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
    source_file_system=None,
    target_file_system=None,
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
    dataset='',
    compatibility_level='',
    workspace=None,
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
    node_family=None,
    auto_scale_enabled=None,
    dynamic_executor_allocation_enabled=None,
    workspace=None,
)
```

### [create_model_bpa_semantic_model](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.create_model_bpa_semantic_model)
```python
import sempy_labs as labs
labs.create_model_bpa_semantic_model(
    dataset=None,
    lakehouse=None,
    lakehouse_workspace=None,
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
    dataset='',
    bim_file='',
    workspace=None,
)
```

### [create_warehouse](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.create_warehouse)
```python
import sempy_labs as labs
labs.create_warehouse(
    warehouse='',
    description=None,
    workspace=None,
)
```

### [delete_custom_pool](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.delete_custom_pool)
```python
import sempy_labs as labs
labs.delete_custom_pool(
    pool_name='',
    workspace=None,
)
```

### [delete_user_from_workspace](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.delete_user_from_workspace)
```python
import sempy_labs as labs
labs.delete_user_from_workspace(
    email_address='',
    workspace=None,
)
```

### [deploy_semantic_model](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.deploy_semantic_model)
```python
import sempy_labs as labs
labs.deploy_semantic_model(
    source_dataset='',
    source_workspace=None,
    target_dataset=None,
    target_workspace=None,
    refresh_target_dataset=None,
)
```

### [deprovision_workspace_identity](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.deprovision_workspace_identity)
```python
import sempy_labs as labs
labs.deprovision_workspace_identity(
    workspace=None,
)
```

### [disable_qso](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.disable_qso)
```python
import sempy_labs as labs
labs.disable_qso(
    dataset='',
    workspace=None,
)
```

### [disconnect_workspace_from_git](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.disconnect_workspace_from_git)
```python
import sempy_labs as labs
labs.disconnect_workspace_from_git(
    workspace=None,
)
```

### [evaluate_dax_impersonation](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.evaluate_dax_impersonation)
```python
import sempy_labs as labs
labs.evaluate_dax_impersonation(
    dataset='',
    dax_query='',
    user_name=None,
    workspace=None,
)
```

### [export_model_to_onelake](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.export_model_to_onelake)
```python
import sempy_labs as labs
labs.export_model_to_onelake(
    dataset='',
    workspace=None,
    destination_lakehouse=None,
    destination_workspace=None,
)
```

### [format_dax_object_name](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.format_dax_object_name)
```python
import sempy_labs as labs
labs.format_dax_object_name(
    table='',
    column='',
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
    workspace=None,
)
```

### [get_capacity_name](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.get_capacity_name)
```python
import sempy_labs as labs
labs.get_capacity_name(
    workspace=None,
)
```

### [get_direct_lake_sql_endpoint](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.get_direct_lake_sql_endpoint)
```python
import sempy_labs as labs
labs.get_direct_lake_sql_endpoint(
    dataset='',
    workspace=None,
)
```

### [get_git_connection](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.get_git_connection)
```python
import sempy_labs as labs
labs.get_git_connection(
    workspace=None,
)
```

### [get_git_status](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.get_git_status)
```python
import sempy_labs as labs
labs.get_git_status(
    workspace=None,
)
```

### [get_measure_dependencies](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.get_measure_dependencies)
```python
import sempy_labs as labs
labs.get_measure_dependencies(
    dataset='',
    workspace=None,
)
```

### [get_model_calc_dependencies](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.get_model_calc_dependencies)
```python
import sempy_labs as labs
labs.get_model_calc_dependencies(
    dataset='',
    workspace=None,
)
```

### [get_notebook_definition](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.get_notebook_definition)
```python
import sempy_labs as labs
labs.get_notebook_definition(
    notebook_name='',
    workspace=None,
    decode=None,
)
```

### [get_object_level_security](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.get_object_level_security)
```python
import sempy_labs as labs
labs.get_object_level_security(
    dataset='',
    workspace=None,
)
```

### [get_semantic_model_bim](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.get_semantic_model_bim)
```python
import sempy_labs as labs
labs.get_semantic_model_bim(
    dataset='',
    workspace=None,
    save_to_file_name=None,
    lakehouse_workspace=None,
)
```

### [get_spark_settings](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.get_spark_settings)
```python
import sempy_labs as labs
labs.get_spark_settings(
    workspace=None,
)
```

### [import_notebook_from_web](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.import_notebook_from_web)
```python
import sempy_labs as labs
labs.import_notebook_from_web(
    notebook_name='',
    url='',
    description=None,
    workspace=None,
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
    workspace=None,
)
```

### [is_default_semantic_model](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.is_default_semantic_model)
```python
import sempy_labs as labs
labs.is_default_semantic_model(
    dataset='',
    workspace=None,
)
```

### [list_capacities](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.list_capacities)
```python
import sempy_labs as labs
labs.list_capacities(
)
```

### [list_custom_pools](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.list_custom_pools)
```python
import sempy_labs as labs
labs.list_custom_pools(
    workspace=None,
)
```

### [list_dashboards](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.list_dashboards)
```python
import sempy_labs as labs
labs.list_dashboards(
    workspace=None,
)
```

### [list_dataflow_storage_accounts](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.list_dataflow_storage_accounts)
```python
import sempy_labs as labs
labs.list_dataflow_storage_accounts(
)
```

### [list_dataflows](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.list_dataflows)
```python
import sempy_labs as labs
labs.list_dataflows(
    workspace=None,
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
labs.list_deployment_pipelines(
)
```

### [list_lakehouses](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.list_lakehouses)
```python
import sempy_labs as labs
labs.list_lakehouses(
    workspace=None,
)
```

### [list_qso_settings](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.list_qso_settings)
```python
import sempy_labs as labs
labs.list_qso_settings(
    dataset=None,
    workspace=None,
)
```

### [list_reports_using_semantic_model](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.list_reports_using_semantic_model)
```python
import sempy_labs as labs
labs.list_reports_using_semantic_model(
    dataset='',
    workspace=None,
)
```

### [list_semantic_model_objects](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.list_semantic_model_objects)
```python
import sempy_labs as labs
labs.list_semantic_model_objects(
    dataset='',
    workspace=None,
)
```

### [list_shortcuts](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.list_shortcuts)
```python
import sempy_labs as labs
labs.list_shortcuts(
    lakehouse=None,
    workspace=None,
)
```

### [list_warehouses](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.list_warehouses)
```python
import sempy_labs as labs
labs.list_warehouses(
    workspace=None,
)
```

### [list_workspace_role_assignments](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.list_workspace_role_assignments)
```python
import sempy_labs as labs
labs.list_workspace_role_assignments(
    workspace=None,
)
```

### [list_workspace_users](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.list_workspace_users)
```python
import sempy_labs as labs
labs.list_workspace_users(
    workspace=None,
)
```

### [measure_dependency_tree](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.measure_dependency_tree)
```python
import sempy_labs as labs
labs.measure_dependency_tree(
    dataset='',
    measure_name='',
    workspace=None,
)
```

### [model_bpa_rules](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.model_bpa_rules)
```python
import sempy_labs as labs
labs.model_bpa_rules(
    dataset='',
    workspace=None,
    dependencies=None,
)
```

### [provision_workspace_identity](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.provision_workspace_identity)
```python
import sempy_labs as labs
labs.provision_workspace_identity(
    workspace=None,
)
```

### [qso_sync](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.qso_sync)
```python
import sempy_labs as labs
labs.qso_sync(
    dataset='',
    workspace=None,
)
```

### [qso_sync_status](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.qso_sync_status)
```python
import sempy_labs as labs
labs.qso_sync_status(
    dataset='',
    workspace=None,
)
```

### [refresh_semantic_model](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.refresh_semantic_model)
```python
import sempy_labs as labs
labs.refresh_semantic_model(
    dataset='',
    tables=None,
    partitions=None,
    refresh_type=None,
    retry_count=None,
    apply_refresh_policy=None,
    max_parallelism=None,
    workspace=None,
)
```

### [resolve_capacity_name](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.resolve_capacity_name)
```python
import sempy_labs as labs
labs.resolve_capacity_name(
    capacity_id=None,
)
```

### [resolve_dataset_id](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.resolve_dataset_id)
```python
import sempy_labs as labs
labs.resolve_dataset_id(
    dataset='',
    workspace=None,
)
```

### [resolve_dataset_name](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.resolve_dataset_name)
```python
import sempy_labs as labs
labs.resolve_dataset_name(
    dataset_id='',
    workspace=None,
)
```

### [resolve_item_type](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.resolve_item_type)
```python
import sempy_labs as labs
labs.resolve_item_type(
    item_id='',
    workspace=None,
)
```

### [resolve_lakehouse_id](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.resolve_lakehouse_id)
```python
import sempy_labs as labs
labs.resolve_lakehouse_id(
    lakehouse='',
    workspace=None,
)
```

### [resolve_lakehouse_name](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.resolve_lakehouse_name)
```python
import sempy_labs as labs
labs.resolve_lakehouse_name(
    lakehouse_id=None,
    workspace=None,
)
```

### [resolve_report_id](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.resolve_report_id)
```python
import sempy_labs as labs
labs.resolve_report_id(
    report='',
    workspace=None,
)
```

### [resolve_report_name](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.resolve_report_name)
```python
import sempy_labs as labs
labs.resolve_report_name(
    report_id='',
    workspace=None,
)
```

### [resolve_workspace_capacity](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.resolve_workspace_capacity)
```python
import sempy_labs as labs
labs.resolve_workspace_capacity(
    workspace=None,
)
```

### [restore_semantic_model](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.restore_semantic_model)
```python
import sempy_labs as labs
labs.restore_semantic_model(
    dataset='',
    file_path='',
    allow_overwrite=None,
    ignore_incompatibilities=None,
    force_restore=None,
    workspace=None,
)
```

### [run_model_bpa](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.run_model_bpa)
```python
import sempy_labs as labs
labs.run_model_bpa(
    dataset='',
    rules=None,
    workspace=None,
    export=None,
    return_dataframe=None,
    extended=None,
    language=None,
)
```

### [run_model_bpa_bulk](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.run_model_bpa_bulk)
```python
import sempy_labs as labs
labs.run_model_bpa_bulk(
    rules=None,
    extended=None,
    language=None,
    workspace=None,
    skip_models=None,
)
```

### [save_as_delta_table](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.save_as_delta_table)
```python
import sempy_labs as labs
labs.save_as_delta_table(
    dataframe='',
    delta_table_name='',
    write_mode='',
    merge_schema=None,
    lakehouse=None,
    workspace=None,
)
```

### [set_qso](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.set_qso)
```python
import sempy_labs as labs
labs.set_qso(
    dataset='',
    auto_sync=None,
    max_read_only_replicas=None,
    workspace=None,
)
```

### [set_semantic_model_storage_format](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.set_semantic_model_storage_format)
```python
import sempy_labs as labs
labs.set_semantic_model_storage_format(
    dataset='',
    storage_format='',
    workspace=None,
)
```

### [set_workspace_default_storage_format](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.set_workspace_default_storage_format)
```python
import sempy_labs as labs
labs.set_workspace_default_storage_format(
    storage_format='',
    workspace=None,
)
```

### [translate_semantic_model](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.translate_semantic_model)
```python
import sempy_labs as labs
labs.translate_semantic_model(
    dataset='',
    languages='',
    exclude_characters=None,
    workspace=None,
)
```

### [unassign_workspace_from_capacity](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.unassign_workspace_from_capacity)
```python
import sempy_labs as labs
labs.unassign_workspace_from_capacity(
    workspace=None,
)
```

### [update_custom_pool](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.update_custom_pool)
```python
import sempy_labs as labs
labs.update_custom_pool(
    pool_name='',
    node_size=None,
    min_node_count=None,
    max_node_count=None,
    min_executors=None,
    max_executors=None,
    node_family=None,
    auto_scale_enabled=None,
    dynamic_executor_allocation_enabled=None,
    workspace=None,
)
```

### [update_from_git](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.update_from_git)
```python
import sempy_labs as labs
labs.update_from_git(
    remote_commit_hash='',
    conflict_resolution_policy='',
    workspace_head=None,
    allow_override=None,
    workspace=None,
)
```

### [update_item](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.update_item)
```python
import sempy_labs as labs
labs.update_item(
    item_type='',
    current_name='',
    new_name='',
    description=None,
    workspace=None,
)
```

### [update_spark_settings](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.update_spark_settings)
```python
import sempy_labs as labs
labs.update_spark_settings(
    automatic_log_enabled=None,
    high_concurrency_enabled=None,
    customize_compute_enabled=None,
    default_pool_name=None,
    max_node_count=None,
    max_executors=None,
    environment_name=None,
    runtime_version=None,
    workspace=None,
)
```

### [update_workspace_user](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.update_workspace_user)
```python
import sempy_labs as labs
labs.update_workspace_user(
    email_address='',
    role_name='',
    principal_type=None,
    workspace=None,
)
```

### [vertipaq_analyzer](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.vertipaq_analyzer)
```python
import sempy_labs as labs
labs.vertipaq_analyzer(
    dataset='',
    workspace=None,
    export=None,
    read_stats_from_data=None,
)
```

### [add_table_to_direct_lake_semantic_model](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.directlake.html#sempy_labs.directlake.add_table_to_direct_lake_semantic_model)
```python
import sempy_labs as labs
import sempy_labs.directlake as directlake
directlake.add_table_to_direct_lake_semantic_model(
    dataset='',
    table_name='',
    lakehouse_table_name='',
    refresh=None,
    workspace=None,
)
```

### [check_fallback_reason](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.directlake.html#sempy_labs.directlake.check_fallback_reason)
```python
import sempy_labs as labs
import sempy_labs.directlake as directlake
directlake.check_fallback_reason(
    dataset='',
    workspace=None,
)
```

### [direct_lake_schema_compare](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.directlake.html#sempy_labs.directlake.direct_lake_schema_compare)
```python
import sempy_labs as labs
import sempy_labs.directlake as directlake
directlake.direct_lake_schema_compare(
    dataset='',
    workspace=None,
)
```

### [direct_lake_schema_sync](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.directlake.html#sempy_labs.directlake.direct_lake_schema_sync)
```python
import sempy_labs as labs
import sempy_labs.directlake as directlake
directlake.direct_lake_schema_sync(
    dataset='',
    workspace=None,
    add_to_model=None,
)
```

### [generate_direct_lake_semantic_model](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.directlake.html#sempy_labs.directlake.generate_direct_lake_semantic_model)
```python
import sempy_labs as labs
import sempy_labs.directlake as directlake
directlake.generate_direct_lake_semantic_model(
    dataset='',
    lakehouse_tables='',
    workspace=None,
    lakehouse=None,
    lakehouse_workspace=None,
    overwrite=None,
    refresh=None,
)
```

### [get_direct_lake_guardrails](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.directlake.html#sempy_labs.directlake.get_direct_lake_guardrails)
```python
import sempy_labs as labs
import sempy_labs.directlake as directlake
directlake.get_direct_lake_guardrails(
)
```

### [get_direct_lake_lakehouse](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.directlake.html#sempy_labs.directlake.get_direct_lake_lakehouse)
```python
import sempy_labs as labs
import sempy_labs.directlake as directlake
directlake.get_direct_lake_lakehouse(
    dataset='',
    workspace=None,
    lakehouse=None,
    lakehouse_workspace=None,
)
```

### [get_direct_lake_source](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.directlake.html#sempy_labs.directlake.get_direct_lake_source)
```python
import sempy_labs as labs
import sempy_labs.directlake as directlake
directlake.get_direct_lake_source(
    dataset='',
    workspace=None,
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
    lakehouse=None,
    workspace=None,
)
```

### [get_sku_size](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.directlake.html#sempy_labs.directlake.get_sku_size)
```python
import sempy_labs as labs
import sempy_labs.directlake as directlake
directlake.get_sku_size(
    workspace=None,
)
```

### [list_direct_lake_model_calc_tables](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.directlake.html#sempy_labs.directlake.list_direct_lake_model_calc_tables)
```python
import sempy_labs as labs
import sempy_labs.directlake as directlake
directlake.list_direct_lake_model_calc_tables(
    dataset='',
    workspace=None,
)
```

### [show_unsupported_direct_lake_objects](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.directlake.html#sempy_labs.directlake.show_unsupported_direct_lake_objects)
```python
import sempy_labs as labs
import sempy_labs.directlake as directlake
directlake.show_unsupported_direct_lake_objects(
    dataset='',
    workspace=None,
)
```

### [update_direct_lake_model_lakehouse_connection](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.directlake.html#sempy_labs.directlake.update_direct_lake_model_lakehouse_connection)
```python
import sempy_labs as labs
import sempy_labs.directlake as directlake
directlake.update_direct_lake_model_lakehouse_connection(
    dataset='',
    workspace=None,
    lakehouse=None,
    lakehouse_workspace=None,
)
```

### [update_direct_lake_partition_entity](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.directlake.html#sempy_labs.directlake.update_direct_lake_partition_entity)
```python
import sempy_labs as labs
import sempy_labs.directlake as directlake
directlake.update_direct_lake_partition_entity(
    dataset='',
    table_name='',
    entity_name='',
    workspace=None,
)
```

### [warm_direct_lake_cache_isresident](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.directlake.html#sempy_labs.directlake.warm_direct_lake_cache_isresident)
```python
import sempy_labs as labs
import sempy_labs.directlake as directlake
directlake.warm_direct_lake_cache_isresident(
    dataset='',
    workspace=None,
)
```

### [warm_direct_lake_cache_perspective](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.directlake.html#sempy_labs.directlake.warm_direct_lake_cache_perspective)
```python
import sempy_labs as labs
import sempy_labs.directlake as directlake
directlake.warm_direct_lake_cache_perspective(
    dataset='',
    perspective='',
    add_dependencies=None,
    workspace=None,
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
    destination_workspace=None,
    shortcut_name=None,
)
```

### [delete_shortcut](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.lakehouse.html#sempy_labs.lakehouse.delete_shortcut)
```python
import sempy_labs as labs
import sempy_labs.lakehouse as lake
lake.delete_shortcut(
    shortcut_name='',
    lakehouse=None,
    workspace=None,
)
```

### [get_lakehouse_columns](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.lakehouse.html#sempy_labs.lakehouse.get_lakehouse_columns)
```python
import sempy_labs as labs
import sempy_labs.lakehouse as lake
lake.get_lakehouse_columns(
    lakehouse=None,
    workspace=None,
)
```

### [get_lakehouse_tables](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.lakehouse.html#sempy_labs.lakehouse.get_lakehouse_tables)
```python
import sempy_labs as labs
import sempy_labs.lakehouse as lake
lake.get_lakehouse_tables(
    lakehouse=None,
    workspace=None,
    extended=None,
    count_rows=None,
    export=None,
)
```

### [lakehouse_attached](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.lakehouse.html#sempy_labs.lakehouse.lakehouse_attached)
```python
import sempy_labs as labs
import sempy_labs.lakehouse as lake
lake.lakehouse_attached(
)
```

### [optimize_lakehouse_tables](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.lakehouse.html#sempy_labs.lakehouse.optimize_lakehouse_tables)
```python
import sempy_labs as labs
import sempy_labs.lakehouse as lake
lake.optimize_lakehouse_tables(
    tables=None,
    lakehouse=None,
    workspace=None,
)
```

### [vacuum_lakehouse_tables](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.lakehouse.html#sempy_labs.lakehouse.vacuum_lakehouse_tables)
```python
import sempy_labs as labs
import sempy_labs.lakehouse as lake
lake.vacuum_lakehouse_tables(
    tables=None,
    lakehouse=None,
    workspace=None,
    retain_n_hours=None,
)
```

### [create_pqt_file](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.migration.html#sempy_labs.migration.create_pqt_file)
```python
import sempy_labs as labs
import sempy_labs.migration as migration
migration.create_pqt_file(
    dataset='',
    workspace=None,
    file_name=None,
)
```

### [migrate_calc_tables_to_lakehouse](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.migration.html#sempy_labs.migration.migrate_calc_tables_to_lakehouse)
```python
import sempy_labs as labs
import sempy_labs.migration as migration
migration.migrate_calc_tables_to_lakehouse(
    dataset='',
    new_dataset='',
    workspace=None,
    new_dataset_workspace=None,
    lakehouse=None,
    lakehouse_workspace=None,
)
```

### [migrate_calc_tables_to_semantic_model](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.migration.html#sempy_labs.migration.migrate_calc_tables_to_semantic_model)
```python
import sempy_labs as labs
import sempy_labs.migration as migration
migration.migrate_calc_tables_to_semantic_model(
    dataset='',
    new_dataset='',
    workspace=None,
    new_dataset_workspace=None,
    lakehouse=None,
    lakehouse_workspace=None,
)
```

### [migrate_field_parameters](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.migration.html#sempy_labs.migration.migrate_field_parameters)
```python
import sempy_labs as labs
import sempy_labs.migration as migration
migration.migrate_field_parameters(
    dataset='',
    new_dataset='',
    workspace=None,
    new_dataset_workspace=None,
)
```

### [migrate_model_objects_to_semantic_model](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.migration.html#sempy_labs.migration.migrate_model_objects_to_semantic_model)
```python
import sempy_labs as labs
import sempy_labs.migration as migration
migration.migrate_model_objects_to_semantic_model(
    dataset='',
    new_dataset='',
    workspace=None,
    new_dataset_workspace=None,
)
```

### [migrate_tables_columns_to_semantic_model](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.migration.html#sempy_labs.migration.migrate_tables_columns_to_semantic_model)
```python
import sempy_labs as labs
import sempy_labs.migration as migration
migration.migrate_tables_columns_to_semantic_model(
    dataset='',
    new_dataset='',
    workspace=None,
    new_dataset_workspace=None,
    lakehouse=None,
    lakehouse_workspace=None,
)
```

### [migration_validation](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.migration.html#sempy_labs.migration.migration_validation)
```python
import sempy_labs as labs
import sempy_labs.migration as migration
migration.migration_validation(
    dataset='',
    new_dataset='',
    workspace=None,
    new_dataset_workspace=None,
)
```

### [refresh_calc_tables](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.migration.html#sempy_labs.migration.refresh_calc_tables)
```python
import sempy_labs as labs
import sempy_labs.migration as migration
migration.refresh_calc_tables(
    dataset='',
    workspace=None,
)
```

### [clone_report](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.report.html#sempy_labs.report.clone_report)
```python
import sempy_labs as labs
import sempy_labs.report as rep
rep.clone_report(
    report='',
    cloned_report='',
    workspace=None,
    target_workspace=None,
    target_dataset=None,
    target_dataset_workspace=None,
)
```

### [create_model_bpa_report](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.report.html#sempy_labs.report.create_model_bpa_report)
```python
import sempy_labs as labs
import sempy_labs.report as rep
rep.create_model_bpa_report(
    report=None,
    dataset=None,
    dataset_workspace=None,
)
```

### [create_report_from_reportjson](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.report.html#sempy_labs.report.create_report_from_reportjson)
```python
import sempy_labs as labs
import sempy_labs.report as rep
rep.create_report_from_reportjson(
    report='',
    dataset='',
    report_json='',
    theme_json=None,
    workspace=None,
)
```

### [export_report](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.report.html#sempy_labs.report.export_report)
```python
import sempy_labs as labs
import sempy_labs.report as rep
rep.export_report(
    report='',
    export_format='',
    file_name=None,
    bookmark_name=None,
    page_name=None,
    visual_name=None,
    report_filter=None,
    workspace=None,
)
```

### [get_report_definition](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.report.html#sempy_labs.report.get_report_definition)
```python
import sempy_labs as labs
import sempy_labs.report as rep
rep.get_report_definition(
    report='',
    workspace=None,
)
```

### [get_report_json](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.report.html#sempy_labs.report.get_report_json)
```python
import sempy_labs as labs
import sempy_labs.report as rep
rep.get_report_json(
    report='',
    workspace=None,
    save_to_file_name=None,
)
```

### [launch_report](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.report.html#sempy_labs.report.launch_report)
```python
import sempy_labs as labs
import sempy_labs.report as rep
rep.launch_report(
    report='',
    workspace=None,
)
```

### [report_rebind](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.report.html#sempy_labs.report.report_rebind)
```python
import sempy_labs as labs
import sempy_labs.report as rep
rep.report_rebind(
    report='',
    dataset='',
    report_workspace=None,
    dataset_workspace=None,
)
```

### [report_rebind_all](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.report.html#sempy_labs.report.report_rebind_all)
```python
import sempy_labs as labs
import sempy_labs.report as rep
rep.report_rebind_all(
    dataset='',
    new_dataset='',
    dataset_workspace=None,
    new_dataset_workpace=None,
    report_workspace=None,
)
```

### [update_report_from_reportjson](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.report.html#sempy_labs.report.update_report_from_reportjson)
```python
import sempy_labs as labs
import sempy_labs.report as rep
rep.update_report_from_reportjson(
    report='',
    report_json='',
    workspace=None,
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
        format_string=None,
        hidden=None,
        description=None,
        display_folder=None,
        data_category=None,
        key=None,
        summarize_by=None,
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
        description=None,
        data_category=None,
        hidden=None,
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
        format_string=None,
        hidden=None,
        description=None,
        display_folder=None,
        data_category=None,
        key=None,
        summarize_by=None,
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
        description=None,
        hidden=None,
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
        ordinal=None,
        description=None,
        format_string_expression=None,
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
        format_string=None,
        hidden=None,
        description=None,
        display_folder=None,
        data_category=None,
        key=None,
        summarize_by=None,
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
        expression=None,
        description=None,
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
        description=None,
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
        object_names='',
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
        levels=None,
        hierarchy_description=None,
        hierarchy_hidden=None,
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
        only_refresh_complete_days=None,
        detect_data_changes_column=None,
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
        mode=None,
        description=None,
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
        format_string=None,
        hidden=None,
        description=None,
        display_folder=None,
        format_string_expression=None,
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
        cross_filtering_behavior=None,
        is_active=None,
        security_filtering_behavior=None,
        rely_on_referential_integrity=None,
)
```

### [add_role](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.add_role)
```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.add_role(
        role_name='',
        model_permission=None,
        description=None,
)
```

### [add_table](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.add_table)
```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.add_table(
        name='',
        description=None,
        data_category=None,
        hidden=None,
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
        object='',
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
    tom.all_calculated_columns(
)
```

### [all_calculated_tables](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.all_calculated_tables)
```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.all_calculated_tables(
)
```

### [all_calculation_groups](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.all_calculation_groups)
```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.all_calculation_groups(
)
```

### [all_calculation_items](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.all_calculation_items)
```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.all_calculation_items(
)
```

### [all_columns](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.all_columns)
```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.all_columns(
)
```

### [all_date_tables](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.all_date_tables)
```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.all_date_tables(
)
```

### [all_hierarchies](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.all_hierarchies)
```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.all_hierarchies(
)
```

### [all_hybrid_tables](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.all_hybrid_tables)
```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.all_hybrid_tables(
)
```

### [all_levels](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.all_levels)
```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.all_levels(
)
```

### [all_measures](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.all_measures)
```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.all_measures(
)
```

### [all_partitions](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.all_partitions)
```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.all_partitions(
)
```

### [all_rls](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.all_rls)
```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.all_rls(
)
```

### [apply_refresh_policy](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.apply_refresh_policy)
```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.apply_refresh_policy(
        table_name='',
        effective_date=None,
        refresh=None,
        max_parallelism=None,
)
```

### [cardinality](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.cardinality)
```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.cardinality(
        column='',
)
```

### [clear_annotations](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.clear_annotations)
```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.clear_annotations(
        object='',
)
```

### [clear_extended_properties](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.clear_extended_properties)
```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.clear_extended_properties(
        object='',
)
```

### [data_size](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.data_size)
```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.data_size(
        column='',
)
```

### [depends_on](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.depends_on)
```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.depends_on(
        object='',
        dependencies='',
)
```

### [dictionary_size](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.dictionary_size)
```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.dictionary_size(
        column='',
)
```

### [fully_qualified_measures](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.fully_qualified_measures)
```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.fully_qualified_measures(
        object='',
        dependencies='',
)
```

### [get_annotation_value](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.get_annotation_value)
```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.get_annotation_value(
        object='',
        name='',
)
```

### [get_annotations](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.get_annotations)
```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.get_annotations(
        object='',
)
```

### [get_extended_properties](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.get_extended_properties)
```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.get_extended_properties(
        object='',
)
```

### [get_extended_property_value](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.get_extended_property_value)
```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.get_extended_property_value(
        object='',
        name='',
)
```

### [has_aggs](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.has_aggs)
```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.has_aggs(
)
```

### [has_date_table](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.has_date_table)
```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.has_date_table(
)
```

### [has_hybrid_table](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.has_hybrid_table)
```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.has_hybrid_table(
)
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
        object='',
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
    tom.is_direct_lake(
)
```

### [is_direct_lake_using_view](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.is_direct_lake_using_view)
```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.is_direct_lake_using_view(
)
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
        object='',
)
```

### [referenced_by](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.referenced_by)
```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.referenced_by(
        object='',
        dependencies='',
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
        object='',
        name='',
)
```

### [remove_extended_property](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.remove_extended_property)
```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.remove_extended_property(
        object='',
        name='',
)
```

### [remove_from_perspective](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.remove_from_perspective)
```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.remove_from_perspective(
        object='',
        perspective_name='',
)
```

### [remove_object](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.remove_object)
```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.remove_object(
        object='',
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
        object='',
        language='',
)
```

### [remove_vertipaq_annotations](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.remove_vertipaq_annotations)
```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.remove_vertipaq_annotations(
)
```

### [row_count](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.row_count)
```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.row_count(
        object='',
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
        base_column=None,
)
```

### [set_annotation](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.set_annotation)
```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.set_annotation(
        object='',
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
        object='',
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
        value=None,
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
        lower_mid_bound=None,
        upper_mid_bound=None,
        status_type=None,
        status_graphic=None,
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
        value=None,
)
```

### [set_translation](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.set_translation)
```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.set_translation(
        object='',
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
    tom.set_vertipaq_annotations(
)
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
        object='',
)
```

### [unqualified_columns](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.unqualified_columns)
```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.unqualified_columns(
        object='',
        dependencies='',
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
        expression=None,
        ordinal=None,
        description=None,
        format_string_expression=None,
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
        source_column=None,
        data_type=None,
        expression=None,
        format_string=None,
        hidden=None,
        description=None,
        display_folder=None,
        data_category=None,
        key=None,
        summarize_by=None,
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
        only_refresh_complete_days=None,
        detect_data_changes_column=None,
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
        expression=None,
        mode=None,
        description=None,
)
```

### [update_measure](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.update_measure)
```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.update_measure(
        measure_name='',
        expression=None,
        format_string=None,
        hidden=None,
        description=None,
        display_folder=None,
        format_string_expression=None,
)
```

### [update_role](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.update_role)
```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.update_role(
        role_name='',
        model_permission=None,
        description=None,
)
```

### [used_in_calc_item](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.used_in_calc_item)
```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.used_in_calc_item(
        object='',
        dependencies='',
)
```

### [used_in_data_coverage_definition](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.used_in_data_coverage_definition)
```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.used_in_data_coverage_definition(
        object='',
        dependencies='',
)
```

### [used_in_hierarchies](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.used_in_hierarchies)
```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.used_in_hierarchies(
        column='',
)
```

### [used_in_levels](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.used_in_levels)
```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.used_in_levels(
        column='',
)
```

### [used_in_relationships](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.used_in_relationships)
```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.used_in_relationships(
        object='',
)
```

### [used_in_rls](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.used_in_rls)
```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.used_in_rls(
        object='',
        dependencies='',
)
```

### [used_in_sort_by](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.used_in_sort_by)
```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.used_in_sort_by(
        column='',
)
```

### [used_size](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.used_size)
```python
import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset='', workspace='', readonly=True) as tom:
    tom.used_size(
        object='',
)
```
