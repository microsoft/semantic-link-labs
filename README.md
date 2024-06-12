# semantic-link-labs

[![PyPI version](https://badge.fury.io/py/semantic-link-labs.svg)](https://badge.fury.io/py/semantic-link-labs)
[![Read The Docs](https://readthedocs.org/projects/semantic-link-labs/badge/?version=0.4.1&style=flat)](https://readthedocs.org/projects/semantic-link-labs/)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![Downloads](https://static.pepy.tech/badge/semantic-link-labs)](https://pepy.tech/project/semantic-link-labs)


This is a python library intended to be used in [Microsoft Fabric notebooks](https://learn.microsoft.com/fabric/data-engineering/how-to-use-notebook). This library was originally intended to contain functions used for [migrating semantic models to Direct Lake mode](https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#direct-lake-migration-1). However, it quickly became apparent that functions within such a library could support many other useful activities in the realm of semantic models, reports, lakehouses and really anything Fabric-related. As such, this library contains a variety of functions ranging from running [Vertipaq Analyzer](https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#vertipaq_analyzer) or the [Best Practice Analyzer](https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#run_model_bpa) against a semantic model to seeing if any [lakehouse tables hit Direct Lake guardrails](https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#get_lakehouse_tables) or accessing the [Tabular Object Model](https://github.com/microsoft/semantic-link-labs/#tabular-object-model-tom) and more!

Instructions for migrating import/DirectQuery semantic models to Direct Lake mode can be found [here](https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#direct-lake-migration-1).

If you encounter any issues, please [raise a bug](https://github.com/microsoft/semantic-link-labs/issues/new?assignees=&labels=&projects=&template=bug_report.md&title=).

If you have ideas for new features/functions, please [request a feature](https://github.com/microsoft/semantic-link-labs/issues/new?assignees=&labels=&projects=&template=feature_request.md&title=).

## Install the .whl file in a Fabric notebook
```python
%pip install "https://raw.githubusercontent.com/microsoft/semantic-link-labs/main/semantic-link-labs-0.4.1-py3-none-any.whl"
```

## Once installed, run this code to import the library into your notebook
```python
import semantic-link-labs as labs
from semantic-link-labs.TOM import connect_semantic_model
```

## Load semantic-link-labs into a custom [Fabric environment](https://learn.microsoft.com/fabric/data-engineering/create-and-use-environment)
An even better way to ensure the semantic-link-labs library is available in your workspace/notebooks is to load it as a library in a custom Fabric environment. If you do this, you will not have to run the above '%pip install' code every time in your notebook. Please follow the steps below.

#### Create a custom environment
1. Navigate to your Fabric workspace
2. Click 'New' -> More options
3. Within 'Data Science', click 'Environment'
4. Name your environment, click 'Create'

#### Add semantic-link-labs as a library to the environment
1. Download the [latest](https://github.com/microsoft/semantic-link-labs/raw/main/semantic-link-labs-0.4.1-py3-none-any.whl) semantic-link-labs library
2. Within 'Custom Libraries', click 'upload'
3. Upload the .whl file which was downloaded in step 1
4. Click 'Save' at the top right of the screen
5. Click 'Publish' at the top right of the screen
6. Click 'Publish All'

#### Update your notebook to use the new environment (*must wait for the environment to finish publishing*)
1. Navigate to your Notebook
2. Select your newly created environment within the 'Environment' drop down in the navigation bar at the top of the notebook

# Function Categories

### Semantic Model
* [clear_cache](https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#clear_cache)
* [create_semantic_model_from_bim](https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#create_semantic_model_from_bim)
* [get_semantic_model_bim](https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#get_semantic_model_bim)
* [get_measure_dependencies](https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#get_measure_dependencies)
* [get_model_calc_dependencies](https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#get_model_calc_dependencies)
* [measure_dependency_tree](https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#measure_dependency_tree)
* [refresh_semantic_model](https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#refresh_semantic_model)
* [cancel_dataset_refresh](https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#cancel_dataset_refresh)
* [run_dax](https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#run_dax)
* [get_object_level_security](https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#get_object_level_security)
* [translate_semantic_model](https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#translate_semantic_model)
* [list_semantic_model_objects](https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#list_semantic_model_objects)

### Report
* [report_rebind](https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#report_rebind)
* [report_rebind_all](https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#report_rebind_all)
* [create_report_from_reportjson](https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#create_report_from_reportjson)
* [get_report_json](https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#get_report_json)
* [export_report](https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#export_report)
* [clone_report](https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#clone_report)
* [list_dashboards](https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#list_dashboards)
* [launch_report](https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#launch_report)
* [generate_embedded_filter](https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#generate_embedded_filter)

### Model Optimization
* [vertipaq_analyzer](https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#vertipaq_analyzer)
* [import_vertipaq_analyzer](https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#import_vertipaq_analyzer)
* [run_model_bpa](https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#run_model_bpa)
* [model_bpa_rules](https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#model_bpa_rules)

### Direct Lake Migration
* [create_pqt_file](https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#create_pqt_file)
* [create_blank_semantic_model](https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#create_blank_semantic_model)
* [migrate_field_parameters](https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#migrate_field_parameters)
* [migrate_tables_columns_to_semantic_model](https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#migrate_tables_columns_to_semantic_model)
* [migrate_calc_tables_to_semantic_model](https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#migrate_calc_tables_to_semantic_model)
* [migrate_model_objects_to_semantic_model](https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#migrate_model_objects_to_semantic_model)
* [migrate_calc_tables_to_lakehouse](https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#migrate_calc_tables_to_lakehouse)
* [refresh_calc_tables](https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#refresh_calc_tables)
* [show_unsupported_direct_lake_objects](https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#show_unsupported_direct_lake_objects)
* [update_direct_lake_partition_entity](https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#update_direct_lake_partition_entity)
* [update_direct_lake_model_lakehouse_connection](https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#update_direct_lake_model_lakehouse_connection)
* [migration_validation](https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#migration_validation)

### Direct Lake
* [check_fallback_reason](https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#check_fallback_reason)
* [control_fallback](https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#control_fallback)
* [direct_lake_schema_compare](https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#direct_lake_schema_compare)
* [direct_lake_schema_sync](https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#direct_lake_schema_sync)
* [get_direct_lake_lakehouse](https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#get_direct_lake_lakehouse)
* [get_directlake_guardrails_for_sku](https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#get_directlake_guardrails_for_sku)
* [get_direct_lake_guardrails](https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#get_direct_lake_guardrails)
* [get_shared_expression](https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#get_shared_expression)
* [get_direct_lake_sql_endpoint](https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#get_direct_lake_sql_endpoint)
* [get_sku_size](https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#get_sku_size)
* [list_direct_lake_model_calc_tables](https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#list_direct_lake_model_calc_tables)
* [warm_direct_lake_cache_perspective](https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#warm_direct_lake_cache_perspective)
* [warm_direct_lake_cache_isresident](https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#warm_direct_lake_cache_isresident)

### Lakehouse
* [get_lakehouse_tables](https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#get_lakehouse_tables)
* [get_lakehouse_columns](https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#get_lakehouse_columns)
* [list_lakehouses](https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#list_lakehouses)
* [export_model_to_onelake](https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#export_model_to_onelake)
* [create_shortcut_onelake](https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#create_shortcut_onelake)
* [delete_shortcut](https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#delete_shortcut)
* [list_shortcuts](https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#list_shortcuts)
* [optimize_lakehouse_tables](https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#optimize_lakehouse_tables)
* [create_warehouse](https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#create_warehouse)
* [update_item](https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#update_item)
* [list_dataflow_storage_accounts](https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#list_dataflow_storage_accounts)
* [list_warehouses](https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#list_warehouses)
* [save_as_delta_table](https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#save_as_delta_table)

### Helper Functions
* [resolve_dataset_id](https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#resolve_dataset_id)
* [resolve_dataset_name](https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#resolve_dataset_name)
* [resolve_lakehouse_id](https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#resolve_lakehouse_id)
* [resolve_lakehouse_name](https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#resolve_lakehouse_name)
* [resolve_report_id](https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#resolve_report_id)
* [resolve_report_name](https://github.com/microsoft/semantic-link-labs?tab=readme-ov-files#resolve_report_name)

### [Tabular Object Model](https://learn.microsoft.com/analysis-services/tom/introduction-to-the-tabular-object-model-tom-in-analysis-services-amo?view=asallproducts-allversions) ([TOM](https://learn.microsoft.com/dotnet/api/microsoft.analysisservices.tabular.model?view=analysisservices-dotnet))
#### 'All' functions for non-parent objects within TOM
* [all_columns](https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#all_columns)
* [all_measures](https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#all_measures)
* [all_partitions](https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#all_partitions)
* [all_hierarchies](https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#all_hierarchies)
* [all_levels](https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#all_levels)
* [all_calculation_items](https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#all_calculation_items)
* [all_rls](https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#all_rls)

#### 'Add' functions
* [add_calculated_column](https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#add_calculated_column)
* [add_calculated_table](https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#add_calculated_table)
* [add_calculated_table_column](https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#add_calculated_table_column)
* [add_calculation_group](https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#add_calculation_group)
* [add_calculation_item](https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#add_calculation_item)
* [add_data_column](https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#add_data_column)
* [add_entity_partition](https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#add_entity_partition)
* [add_expression](https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#add_expression)
* [add_field_parameter](https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#add_field_parameter)
* [add_hierarchy](https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#add_hierarchy)
* [add_m_partition](https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#add_m_partition)
* [add_measure](https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#add_measure)
* [add_perspective](https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#add_perspective)
* [add_relationship](https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#add_relationship)
* [add_role](https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#add_role)
* [add_table](https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#add_table)
* [add_translation](https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#add_translation)

#### 'Set' functions
* [set_direct_lake_behavior](https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#set_direct_lake_behavior)
* [set_is_available_in_mdx](https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#set_is_available_in_mdx)
* [set_ols](https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#set_ols)
* [set_rls](https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#add_rls)
* [set_summarize_by](https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#set_summarize_by)
* [set_translation](https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#set_translation)

#### 'Remove' functions
* [remove_object](https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#remove_object)
* [remove_translation](https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#remove_translation)

#### 'Used-in' and dependency functions
* [used_in_relationships](https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#used_in_relationships)
* [used_in_hierarchies](https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#used_in_hierarchies)
* [used_in_levels](https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#used_in_levels)
* [used_in_sort_by](https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#used_in_sort_by)
* [used_in_rls](https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#used_in_rls)
* [used_in_calc_item](https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#used_in_calc_item)
* [depends_on](https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#depends_on)
* [referenced_by](https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#referenced_by)
* [fully_qualified_measures](https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#fully_qualified_measures)
* [unqualified_columns](https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#unqualified_columns)

#### Vertipaq Analyzer data functions
* [remove_vertipaq_annotations](https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#remove_vertipaq_annotations)
* [set_vertipaq_annotations](https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#set_vertipaq_annotations)
* [row_count](https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#row_count)
* [used_size](https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#used_size)
* [data_size](https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#data_size)
* [dictionary_size](https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#dictionary_size)
* [total_size](https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#total_size)
* [cardinality](https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#cardinality)

#### Perspectives
* [in_perspective](https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#in_perspective)
* [add_to_perspective](https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#add_to_perspective)
* [remove_from_perspective](https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#remove_from_perspective)
  
#### Annotations
* [get_annotations](https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#get_annotations)
* [set_annotation](https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#set_annotation)
* [get_annotation_value](https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#get_annotation_value)
* [remove_annotation](https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#remove_annotation)
* [clear_annotations](https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#clear_annotations)

#### Extended Properties
* [get_extended_properties](https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#get_extended_properties)
* [set_extended_property](https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#set_extended_property)
* [get_extended_property_value](https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#get_extended_property_value)
* [remove_extended_property](https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#remove_extended_property)
* [clear_extended_properties](https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#clear_extended_properties)

#### Misc
* [is_direct_lake](https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#is_direct_lake)
* [is_field_parameter](https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#is_field_parameter)


---
## Direct Lake migration

The following process automates the migration of an import/DirectQuery model to a new [Direct Lake](https://learn.microsoft.com/power-bi/enterprise/directlake-overview) model. The first step is specifically applicable to models which use Power Query to perform data transformations. If your model does not use Power Query, you must migrate the base tables used in your semantic model to a Fabric lakehouse.

Check out [Nikola Ilic](https://twitter.com/DataMozart)'s terrific [blog post](https://data-mozart.com/migrate-existing-power-bi-semantic-models-to-direct-lake-a-step-by-step-guide/) on this topic!

Check out my [blog post](https://www.elegantbi.com/post/direct-lake-migration) on this topic!

### Prerequisites

* Make sure you [enable XMLA Read/Write](https://learn.microsoft.com/power-bi/enterprise/service-premium-connect-tools#enable-xmla-read-write) for your capacity
* Make sure you have a [lakehouse](https://learn.microsoft.com/fabric/onelake/create-lakehouse-onelake#create-a-lakehouse) in a Fabric workspace
* Enable the following [setting](https://learn.microsoft.com/power-bi/transform-model/service-edit-data-models#enable-the-preview-feature): Workspace -> Workspace Settings -> General -> Data model settings -> Users can edit data models in the Power BI service

### Instructions

1. Download this [notebook](https://github.com/microsoft/semantic-link-labs/blob/main/Migration%20to%20Direct%20Lake.ipynb). **Use version 0.2.1 or higher only.**
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
