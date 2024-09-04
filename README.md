
# Semantic Link Labs

[![PyPI version](https://badge.fury.io/py/semantic-link-labs.svg)](https://badge.fury.io/py/semantic-link-labs)
[![Read The Docs](https://readthedocs.org/projects/semantic-link-labs/badge/?version=0.7.2&style=flat)](https://readthedocs.org/projects/semantic-link-labs/)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![Downloads](https://static.pepy.tech/badge/semantic-link-labs)](https://pepy.tech/project/semantic-link-labs)

---
[Read the documentation on ReadTheDocs!](https://semantic-link-labs.readthedocs.io/en/stable/)
---

This is a python library intended to be used in [Microsoft Fabric notebooks](https://learn.microsoft.com/fabric/data-engineering/how-to-use-notebook). This library was originally intended to solely contain functions used for [migrating semantic models to Direct Lake mode](https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#direct-lake-migration). However, it quickly became apparent that functions within such a library could support many other useful activities in the realm of semantic models, reports, lakehouses and really anything Fabric-related. As such, this library contains a variety of functions ranging from running [Vertipaq Analyzer](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.import_vertipaq_analyzer) or the [Best Practice Analyzer](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.run_model_bpa) against a semantic model to seeing if any [lakehouse tables hit Direct Lake guardrails](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.lakehouse.html#sempy_labs.lakehouse.get_lakehouse_tables) or accessing the [Tabular Object Model](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html) and more!

Instructions for migrating import/DirectQuery semantic models to Direct Lake mode can be found [here](https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#direct-lake-migration).

If you encounter any issues, please [raise a bug](https://github.com/microsoft/semantic-link-labs/issues/new?assignees=&labels=&projects=&template=bug_report.md&title=).

If you have ideas for new features/functions, please [request a feature](https://github.com/microsoft/semantic-link-labs/issues/new?assignees=&labels=&projects=&template=feature_request.md&title=).

## Install the library in a Fabric notebook
```python
%pip install semantic-link-labs
```

## Once installed, run this code to import the library into your notebook
```python
import sempy_labs as labs
from sempy_labs import migration, directlake
from sempy_labs import lakehouse as lake
from sempy_labs import report as rep
from sempy_labs.tom import connect_semantic_model
```

## Load semantic-link-labs into a custom [Fabric environment](https://learn.microsoft.com/fabric/data-engineering/create-and-use-environment)
An even better way to ensure the semantic-link-labs library is available in your workspace/notebooks is to load it as a library in a custom Fabric environment. If you do this, you will not have to run the above '%pip install' code every time in your notebook. Please follow the steps below.

#### Create a custom environment
1. Navigate to your Fabric workspace
2. Click 'New' -> More options
3. Within 'Data Science', click 'Environment'
4. Name your environment, click 'Create'

#### Add semantic-link-labs as a library to the environment
1. Within 'Public libraries', click 'Add from PyPI'
2. Enter 'semantic-link-labs'.
3. Click 'Save' at the top right of the screen
4. Click 'Publish' at the top right of the screen
5. Click 'Publish All'

#### Update your notebook to use the new environment (*must wait for the environment to finish publishing*)
1. Navigate to your Notebook
2. Select your newly created environment within the 'Environment' drop down in the navigation bar at the top of the notebook



## Function Categories

### Admin
* [add_user_to_workspace](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#add_user_to_workspace)
* [assign_workspace_to_capacity](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#assign_workspace_to_capacity)
* [assign_workspace_to_dataflow_storage](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#assign_workspace_to_dataflow_storage)
* [delete_user_from_workspace](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#delete_user_from_workspace)
* [deprovision_workspace_identity](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#deprovision_workspace_identity)
* [list_workspace_role_assignments](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#list_workspace_role_assignments)
* [list_workspace_users](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#list_workspace_users)
* [provision_workspace_identity](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#provision_workspace_identity)
* [set_workspace_default_storage_format](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#set_workspace_default_storage_format)
* [unassign_workspace_from_capacity](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#unassign_workspace_from_capacity)
* [update_workspace_user](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#update_workspace_user)

### Deployment Pipelines
* [list_deployment_pipeline_stage_items](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#list_deployment_pipeline_stage_items)
* [list_deployment_pipeline_stages](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#list_deployment_pipeline_stages)
* [list_deployment_pipelines](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#list_deployment_pipelines)

### Direct Lake
* [add_table_to_direct_lake_semantic_model](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#add_table_to_direct_lake_semantic_model)
* [check_fallback_reason](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#check_fallback_reason)
* [direct_lake_schema_compare](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#direct_lake_schema_compare)
* [direct_lake_schema_sync](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#direct_lake_schema_sync)
* [generate_direct_lake_semantic_model](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#generate_direct_lake_semantic_model)
* [get_direct_lake_guardrails](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#get_direct_lake_guardrails)
* [get_direct_lake_lakehouse](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#get_direct_lake_lakehouse)
* [get_direct_lake_source](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#get_direct_lake_source)
* [get_direct_lake_sql_endpoint](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#get_direct_lake_sql_endpoint)
* [get_directlake_guardrails_for_sku](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#get_directlake_guardrails_for_sku)
* [get_shared_expression](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#get_shared_expression)
* [get_sku_size](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#get_sku_size)
* [list_direct_lake_model_calc_tables](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#list_direct_lake_model_calc_tables)
* [show_unsupported_direct_lake_objects](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#show_unsupported_direct_lake_objects)
* [update_direct_lake_model_lakehouse_connection](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#update_direct_lake_model_lakehouse_connection)
* [update_direct_lake_partition_entity](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#update_direct_lake_partition_entity)
* [warm_direct_lake_cache_isresident](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#warm_direct_lake_cache_isresident)
* [warm_direct_lake_cache_perspective](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#warm_direct_lake_cache_perspective)

### Direct Lake Migration
* [create_pqt_file](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#create_pqt_file)
* [migrate_calc_tables_to_lakehouse](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#migrate_calc_tables_to_lakehouse)
* [migrate_calc_tables_to_semantic_model](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#migrate_calc_tables_to_semantic_model)
* [migrate_field_parameters](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#migrate_field_parameters)
* [migrate_model_objects_to_semantic_model](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#migrate_model_objects_to_semantic_model)
* [migrate_tables_columns_to_semantic_model](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#migrate_tables_columns_to_semantic_model)
* [migration_validation](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#migration_validation)
* [refresh_calc_tables](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#refresh_calc_tables)

### General
* [create_warehouse](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#create_warehouse)
* [get_notebook_definition](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#get_notebook_definition)
* [import_notebook_from_web](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#import_notebook_from_web)
* [list_capacities](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#list_capacities)
* [list_dashboards](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#list_dashboards)
* [list_dataflow_storage_accounts](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#list_dataflow_storage_accounts)
* [list_dataflows](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#list_dataflows)
* [list_warehouses](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#list_warehouses)
* [update_item](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#update_item)

### Git
* [commit_to_git](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#commit_to_git)
* [connect_workspace_to_git](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#connect_workspace_to_git)
* [disconnect_workspace_from_git](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#disconnect_workspace_from_git)
* [get_git_connection](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#get_git_connection)
* [get_git_status](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#get_git_status)
* [initialize_git_connection](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#initialize_git_connection)
* [update_from_git](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#update_from_git)

### Helper Functions
* [create_abfss_path](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#create_abfss_path)
* [create_relationship_name](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#create_relationship_name)
* [format_dax_object_name](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#format_dax_object_name)
* [generate_embedded_filter](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#generate_embedded_filter)
* [get_capacity_id](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#get_capacity_id)
* [get_capacity_name](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#get_capacity_name)
* [resolve_capacity_name](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#resolve_capacity_name)
* [resolve_dataset_id](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#resolve_dataset_id)
* [resolve_dataset_name](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#resolve_dataset_name)
* [resolve_item_type](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#resolve_item_type)
* [resolve_report_id](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#resolve_report_id)
* [resolve_report_name](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#resolve_report_name)
* [resolve_workspace_capacity](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#resolve_workspace_capacity)
* [save_as_delta_table](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#save_as_delta_table)

### Lakehouse
* [create_shortcut_onelake](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#create_shortcut_onelake)
* [delete_shortcut](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#delete_shortcut)
* [export_model_to_onelake](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#export_model_to_onelake)
* [get_lakehouse_columns](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#get_lakehouse_columns)
* [get_lakehouse_tables](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#get_lakehouse_tables)
* [lakehouse_attached](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#lakehouse_attached)
* [list_lakehouses](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#list_lakehouses)
* [list_shortcuts](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#list_shortcuts)
* [optimize_lakehouse_tables](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#optimize_lakehouse_tables)
* [resolve_lakehouse_id](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#resolve_lakehouse_id)
* [resolve_lakehouse_name](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#resolve_lakehouse_name)
* [vacuum_lakehouse_tables](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#vacuum_lakehouse_tables)

### Model Optimization
* [import_vertipaq_analyzer](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#import_vertipaq_analyzer)
* [model_bpa_rules](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#model_bpa_rules)
* [run_model_bpa](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#run_model_bpa)
* [run_model_bpa_bulk](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#run_model_bpa_bulk)
* [vertipaq_analyzer](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#vertipaq_analyzer)

### Query Scale Out
* [disable_qso](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#disable_qso)
* [list_qso_settings](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#list_qso_settings)
* [qso_sync](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#qso_sync)
* [qso_sync_status](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#qso_sync_status)
* [set_qso](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#set_qso)

### Report
* [clone_report](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#clone_report)
* [create_model_bpa_report](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#create_model_bpa_report)
* [create_report_from_reportjson](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#create_report_from_reportjson)
* [export_report](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#export_report)
* [get_report_definition](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#get_report_definition)
* [get_report_json](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#get_report_json)
* [launch_report](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#launch_report)
* [report_rebind](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#report_rebind)
* [report_rebind_all](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#report_rebind_all)
* [update_report_from_reportjson](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#update_report_from_reportjson)

### Semantic Model
* [backup_semantic_model](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#backup_semantic_model)
* [cancel_dataset_refresh](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#cancel_dataset_refresh)
* [clear_cache](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#clear_cache)
* [copy_semantic_model_backup_file](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#copy_semantic_model_backup_file)
* [create_blank_semantic_model](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#create_blank_semantic_model)
* [create_model_bpa_semantic_model](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#create_model_bpa_semantic_model)
* [create_semantic_model_from_bim](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#create_semantic_model_from_bim)
* [deploy_semantic_model](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#deploy_semantic_model)
* [evaluate_dax_impersonation](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#evaluate_dax_impersonation)
* [get_measure_dependencies](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#get_measure_dependencies)
* [get_model_calc_dependencies](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#get_model_calc_dependencies)
* [get_object_level_security](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#get_object_level_security)
* [get_semantic_model_bim](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#get_semantic_model_bim)
* [is_default_semantic_model](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#is_default_semantic_model)
* [list_reports_using_semantic_model](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#list_reports_using_semantic_model)
* [list_semantic_model_objects](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#list_semantic_model_objects)
* [measure_dependency_tree](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#measure_dependency_tree)
* [refresh_semantic_model](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#refresh_semantic_model)
* [restore_semantic_model](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#restore_semantic_model)
* [set_semantic_model_storage_format](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#set_semantic_model_storage_format)
* [translate_semantic_model](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#translate_semantic_model)

### Spark
* [create_custom_pool](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#create_custom_pool)
* [delete_custom_pool](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#delete_custom_pool)
* [get_spark_settings](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#get_spark_settings)
* [list_custom_pools](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#list_custom_pools)
* [update_custom_pool](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#update_custom_pool)
* [update_spark_settings](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#update_spark_settings)

### [Tabular Object Model](https://learn.microsoft.com/analysis-services/tom/introduction-to-the-tabular-object-model-tom-in-analysis-services-amo?view=asallproducts-allversions) ([TOM](https://learn.microsoft.com/dotnet/api/microsoft.analysisservices.tabular.model?view=analysisservices-dotnet))
#### 'Add' functions
* [add_calculated_column](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#add_calculated_column)
* [add_calculated_table](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#add_calculated_table)
* [add_calculated_table_column](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#add_calculated_table_column)
* [add_calculation_group](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#add_calculation_group)
* [add_calculation_item](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#add_calculation_item)
* [add_data_column](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#add_data_column)
* [add_entity_partition](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#add_entity_partition)
* [add_expression](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#add_expression)
* [add_field_parameter](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#add_field_parameter)
* [add_hierarchy](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#add_hierarchy)
* [add_m_partition](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#add_m_partition)
* [add_measure](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#add_measure)
* [add_relationship](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#add_relationship)
* [add_role](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#add_role)
* [add_table](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#add_table)
* [add_time_intelligence](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#add_time_intelligence)

#### 'All' functions
* [all_calculated_columns](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#all_calculated_columns)
* [all_calculated_tables](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#all_calculated_tables)
* [all_calculation_groups](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#all_calculation_groups)
* [all_calculation_items](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#all_calculation_items)
* [all_columns](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#all_columns)
* [all_date_tables](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#all_date_tables)
* [all_hierarchies](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#all_hierarchies)
* [all_hybrid_tables](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#all_hybrid_tables)
* [all_levels](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#all_levels)
* [all_measures](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#all_measures)
* [all_partitions](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#all_partitions)
* [all_rls](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#all_rls)

#### 'Remove' functions
* [remove_alternate_of](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#remove_alternate_of)
* [remove_object](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#remove_object)
* [remove_sort_by_column](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#remove_sort_by_column)

#### 'Set' functions
* [set_aggregations](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#set_aggregations)
* [set_alternate_of](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#set_alternate_of)
* [set_data_coverage_definition](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#set_data_coverage_definition)
* [set_data_type](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#set_data_type)
* [set_direct_lake_behavior](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#set_direct_lake_behavior)
* [set_encoding_hint](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#set_encoding_hint)
* [set_is_available_in_mdx](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#set_is_available_in_mdx)
* [set_kpi](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#set_kpi)
* [set_ols](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#set_ols)
* [set_rls](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#set_rls)
* [set_sort_by_column](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#set_sort_by_column)
* [set_summarize_by](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#set_summarize_by)

#### 'Update' functions
* [update_calculation_item](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#update_calculation_item)
* [update_column](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#update_column)
* [update_m_partition](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#update_m_partition)
* [update_measure](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#update_measure)
* [update_role](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#update_role)

#### 'Used-in' and dependency functions
* [depends_on](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#depends_on)
* [fully_qualified_measures](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#fully_qualified_measures)
* [referenced_by](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#referenced_by)
* [unqualified_columns](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#unqualified_columns)
* [used_in_calc_item](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#used_in_calc_item)
* [used_in_data_coverage_definition](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#used_in_data_coverage_definition)
* [used_in_hierarchies](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#used_in_hierarchies)
* [used_in_levels](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#used_in_levels)
* [used_in_relationships](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#used_in_relationships)
* [used_in_rls](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#used_in_rls)
* [used_in_sort_by](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#used_in_sort_by)

#### Annotations
* [clear_annotations](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#clear_annotations)
* [get_annotation_value](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#get_annotation_value)
* [get_annotations](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#get_annotations)
* [remove_annotation](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#remove_annotation)
* [set_annotation](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#set_annotation)

#### Extended Properties
* [clear_extended_properties](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#clear_extended_properties)
* [get_extended_properties](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#get_extended_properties)
* [get_extended_property_value](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#get_extended_property_value)
* [remove_extended_property](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#remove_extended_property)
* [set_extended_property](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#set_extended_property)

#### Incremental Refresh
* [add_incremental_refresh_policy](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#add_incremental_refresh_policy)
* [apply_refresh_policy](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#apply_refresh_policy)
* [has_incremental_refresh_policy](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#has_incremental_refresh_policy)
* [show_incremental_refresh_policy](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#show_incremental_refresh_policy)
* [update_incremental_refresh_policy](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#update_incremental_refresh_policy)

#### Misc functions
* [has_aggs](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#has_aggs)
* [has_date_table](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#has_date_table)
* [has_hybrid_table](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#has_hybrid_table)
* [is_agg_table](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#is_agg_table)
* [is_auto_date_table](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#is_auto_date_table)
* [is_calculated_table](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#is_calculated_table)
* [is_date_table](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#is_date_table)
* [is_direct_lake](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#is_direct_lake)
* [is_direct_lake_using_view](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#is_direct_lake_using_view)
* [is_field_parameter](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#is_field_parameter)
* [is_hybrid_table](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#is_hybrid_table)
* [mark_as_date_table](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#mark_as_date_table)

#### Perspectives
* [add_perspective](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#add_perspective)
* [add_to_perspective](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#add_to_perspective)
* [in_perspective](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#in_perspective)
* [remove_from_perspective](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#remove_from_perspective)

#### Translations
* [add_translation](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#add_translation)
* [remove_translation](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#remove_translation)
* [set_translation](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#set_translation)

#### Vertipaq Stats
* [cardinality](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#cardinality)
* [data_size](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#data_size)
* [dictionary_size](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#dictionary_size)
* [records_per_segment](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#records_per_segment)
* [remove_vertipaq_annotations](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#remove_vertipaq_annotations)
* [row_count](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#row_count)
* [set_vertipaq_annotations](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#set_vertipaq_annotations)
* [total_size](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#total_size)
* [used_size](https://github.com/microsoft/semantic-link-labs/blob/main/function_examples.md#used_size)


---
## Direct Lake migration

The following process automates the migration of an import/DirectQuery model to a new [Direct Lake](https://learn.microsoft.com/power-bi/enterprise/directlake-overview) model. The first step is specifically applicable to models which use Power Query to perform data transformations. If your model does not use Power Query, you must migrate the base tables used in your semantic model to a Fabric lakehouse.

Check out [Nikola Ilic](https://twitter.com/DataMozart)'s terrific [blog post](https://data-mozart.com/migrate-existing-power-bi-semantic-models-to-direct-lake-a-step-by-step-guide/) on this topic!

Check out my [blog post](https://www.elegantbi.com/post/direct-lake-migration) on this topic!

[![Direct Lake Migration Video](https://img.youtube.com/vi/gGIxMrTVyyI/0.jpg)](https://www.youtube.com/watch?v=gGIxMrTVyyI?t=495)

### Prerequisites

* Make sure you [enable XMLA Read/Write](https://learn.microsoft.com/power-bi/enterprise/service-premium-connect-tools#enable-xmla-read-write) for your capacity
* Make sure you have a [lakehouse](https://learn.microsoft.com/fabric/onelake/create-lakehouse-onelake#create-a-lakehouse) in a Fabric workspace
* Enable the following [setting](https://learn.microsoft.com/power-bi/transform-model/service-edit-data-models#enable-the-preview-feature): Workspace -> Workspace Settings -> General -> Data model settings -> Users can edit data models in the Power BI service

### Instructions

1. Download this [notebook](https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Migration%20to%20Direct%20Lake.ipynb).
2. Make sure you are in the ['Data Engineering' persona](https://learn.microsoft.com/fabric/get-started/microsoft-fabric-overview#components-of-microsoft-fabric). Click the icon at the bottom left corner of your Workspace screen and select 'Data Engineering'
3. In your workspace, select 'New -> Import notebook' and import the notebook from step 1.
4. [Add your lakehouse](https://learn.microsoft.com/fabric/data-engineering/lakehouse-notebook-explore#add-or-remove-a-lakehouse) to your Fabric notebook
5. Follow the instructions within the notebook.

### The migration process

> [!NOTE]
> The first 4 steps are only necessary if you have logic in Power Query. Otherwise, you will need to migrate your semantic model source tables to lakehouse tables.

1. The first step of the notebook creates a Power Query Template (.pqt) file which eases the migration of Power Query logic to Dataflows Gen2.
2. After the .pqt file is created, sync files from your [OneLake file explorer](https://www.microsoft.com/download/details.aspx?id=105222)
3. Navigate to your lakehouse (this is critical!). From your lakehouse, create a new Dataflows Gen2, and import the Power Query Template file. Doing this step from your lakehouse will automatically set the destination for all tables to this lakehouse (instead of having to manually map each one).
4. Publish the Dataflow Gen2 and wait for it to finish creating the delta lake tables in your lakehouse.
5. Back in the notebook, the next step will create your new Direct Lake semantic model with the name of your choice, taking all the relevant properties from the orignal semantic model and refreshing/framing your new semantic model.

> [!NOTE]
> As of version 0.2.1, calculated tables are also migrated to Direct Lake (as data tables with their DAX expression stored as model annotations in the new semantic model). Additionally, Field Parameters are migrated as they were in the original semantic model (as a calculated table).

6. Finally, you can easily rebind your all reports which use the import/DQ semantic model to the new Direct Lake semantic model in one click.

### Completing these steps will do the following:
* Offload your Power Query logic to Dataflows Gen2 inside of Fabric (where it can be maintained and development can continue).
* Dataflows Gen2 will create delta tables in your Fabric lakehouse. These tables can then be used for your Direct Lake model.
* Create a new semantic model in Direct Lake mode containing all the standard tables and columns, calculation groups, measures, relationships, hierarchies, roles, row level security, perspectives, and translations from your original semantic model.
* Viable calculated tables are migrated to the new semantic model as data tables. Delta tables are dynamically generated in the lakehouse to support the Direct Lake model. The calculated table DAX logic is stored as model annotations in the new semantic model.
* Field parameters are migrated to the new semantic model as they were in the original semantic model (as calculated tables). Any calculated columns used in field parameters are automatically removed in the new semantic model's field parameter(s).
* Non-supported objects are not transferred (i.e. calculated columns, relationships using columns with unsupported data types etc.).
* Reports used by your original semantic model will be rebinded to your new semantic model.

## Contributing

This project welcomes contributions and suggestions.  Most contributions require you to agree to a
Contributor License Agreement (CLA) declaring that you have the right to, and actually do, grant us
the rights to use your contribution. For details, visit https://cla.opensource.microsoft.com.

When you submit a pull request, a CLA bot will automatically determine whether you need to provide
a CLA and decorate the PR appropriately (e.g., status check, comment). Simply follow the instructions
provided by the bot. You will only need to do this once across all repos using our CLA.

This project has adopted the [Microsoft Open Source Code of Conduct](https://opensource.microsoft.com/codeofconduct/).
For more information see the [Code of Conduct FAQ](https://opensource.microsoft.com/codeofconduct/faq/) or
contact [opencode@microsoft.com](mailto:opencode@microsoft.com) with any additional questions or comments.

## Trademarks

This project may contain trademarks or logos for projects, products, or services. Authorized use of Microsoft 
trademarks or logos is subject to and must follow 
[Microsoft's Trademark & Brand Guidelines](https://www.microsoft.com/en-us/legal/intellectualproperty/trademarks/usage/general).
Use of Microsoft trademarks or logos in modified versions of this project must not cause confusion or imply Microsoft sponsorship.
Any use of third-party trademarks or logos are subject to those third-party's policies.
