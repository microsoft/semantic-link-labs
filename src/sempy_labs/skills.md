# Sempy Chat Skills

This file defines intent-override rules for the Sempy Chat copilot.
Each rule maps specific phrases in a user's prompt to a fixed function,
bypassing the normal scoring algorithm.

## Rules

To add a new rule, copy an existing block and update the patterns and function name.
Each rule needs a `Patterns` line (comma-separated trigger phrases) and a `Function` line (exact function name).

---

## Semantic Modeling

### Run Model BPA
- **Patterns**: model bpa, best practice analyzer, run bpa
- **Function**: run_model_bpa

### Vertipaq Analyzer
- **Patterns**: vertipaq, verti paq, verti-paq
- **Function**: vertipaq_analyzer

### VPAX
- **Patterns**: vpax, .vpax, create vpax
- **Function**: create_vpax

### Format DAX
- **Patterns**: format dax, dax format, dax formatter
- **Function**: tom.format_dax
- **Code**:
```python
from sempy_labs.tom import connect_semantic_model

with connect_semantic_model(dataset={dataset}, readonly=False, workspace={workspace}) as tom:
    tom.format_dax()
```

### Translate Semantic Model
- **Patterns**: translate, translate semantic model, translate model, translate dataset, translation
- **Function**: translate_semantic_model
- **Code**:
```python
import sempy_labs as labs

labs.translate_semantic_model(dataset={dataset}, languages={languages}, workspace={workspace})
```

### Refresh Semantic Model
- **Patterns**: refresh semantic model, refresh model, refresh dataset
- **Function**: refresh_semantic_model

### Backup Semantic Model
- **Patterns**: backup semantic model, backup model, backup dataset
- **Function**: backup_semantic_model

### Restore Semantic Model
- **Patterns**: restore semantic model, restore model, restore dataset
- **Function**: restore_semantic_model

### Deploy Semantic Model
- **Patterns**: deploy semantic model, deploy model, deploy dataset
- **Function**: deploy_semantic_model

### Semantic Model Size
- **Patterns**: semantic model size, model size, dataset size, size of model
- **Function**: get_semantic_model_size

### Auto-generate Measure Descriptions
- **Patterns**: generate measure description, generate description, auto generate description, measure description
- **Function**: tom.generate_measure_descriptions
- **Code**:
```python
from sempy_labs.tom import connect_semantic_model

with connect_semantic_model(dataset={dataset}, readonly=False, workspace={workspace}) as tom:
    tom.generate_measure_descriptions()
```

### DAX Query Dependencies
- **Patterns**: dax query dependencies, dax dependencies, columns used by dax
- **Function**: get_dax_query_dependencies

### List Synonyms
- **Patterns**: list synonyms, synonyms, linguistic metadata
- **Function**: list_synonyms

### Is Default Semantic Model
- **Patterns**: default semantic model, is default model, is default dataset
- **Function**: is_default_semantic_model

---

## Tabular Object Model (TOM)

### Connect to Semantic Model
- **Patterns**: connect semantic model, connect to model, connect tom, tabular object model
- **Function**: tom.connect_semantic_model
- **Code**:
```python
from sempy_labs.tom import connect_semantic_model

with connect_semantic_model(dataset={dataset}, readonly=True, workspace={workspace}) as tom:
    for t in tom.model.Tables:
        for c in t.Columns:
            print(f"'{t.Name}'[{c.Name}]")
```

### Set Vertipaq Annotations
- **Patterns**: set vertipaq annotation, vertipaq annotation, save vertipaq
- **Function**: tom.set_vertipaq_annotations
- **Code**:
```python
from sempy_labs.tom import connect_semantic_model

with connect_semantic_model(dataset={dataset}, readonly=False, workspace={workspace}) as tom:
    tom.set_vertipaq_annotations()
```

### Show Incremental Refresh Policy
- **Patterns**: incremental refresh, refresh policy, show incremental refresh
- **Function**: tom.show_incremental_refresh_policy
- **Code**:
```python
from sempy_labs.tom import connect_semantic_model

with connect_semantic_model(dataset={dataset}, readonly=True, workspace={workspace}) as tom:
    tom.show_incremental_refresh_policy(table_name=...)
```

### Set Translation
- **Patterns**: set translation, add translation
- **Function**: tom.set_translation
- **Code**:
```python
from sempy_labs.tom import connect_semantic_model

with connect_semantic_model(dataset={dataset}, readonly=False, workspace={workspace}) as tom:
    t = tom.model.Tables["TableName"]
    tom.set_translation(object=t, language="fr-FR", property="Name", value="TranslatedName")
```

### Create Field Parameter
- **Patterns**: field parameter, create field parameter, add field parameter
- **Function**: tom.add_field_parameter
- **Code**:
```python
from sempy_labs.tom import connect_semantic_model

with connect_semantic_model(dataset={dataset}, readonly=False, workspace={workspace}) as tom:
    tom.add_field_parameter(table_name="ParameterName", objects=["'Table'[Column]", "[Measure]"])
```

### Set IsAvailableInMdx
- **Patterns**: isavailableinmdx, available in mdx, set mdx
- **Function**: tom.set_isavailableinmdx
- **Code**:
```python
from sempy_labs.tom import connect_semantic_model

with connect_semantic_model(dataset={dataset}, readonly=False, workspace={workspace}) as tom:
    for c in tom.all_columns():
        if c.IsAvailableInMDX and (c.IsHidden or c.Parent.IsHidden):
            c.IsAvailableInMDX = False
```

### Set M Expression
- **Patterns**: set expression, set parameter, m expression, set m expression
- **Function**: tom.set_expression
- **Code**:
```python
from sempy_labs.tom import connect_semantic_model

with connect_semantic_model(dataset={dataset}, readonly=False, workspace={workspace}) as tom:
    tom.model.Expressions["ParameterName"].Expression = "new_expression_value"
```

### Set RLS
- **Patterns**: row level security, set rls, add rls, add role
- **Function**: tom.set_rls
- **Code**:
```python
from sempy_labs.tom import connect_semantic_model

with connect_semantic_model(dataset={dataset}, readonly=False, workspace={workspace}) as tom:
    tom.add_role(role_name="RoleName", model_permission="Reader")
    tom.set_rls(
        role_name="RoleName",
        table_name="TableName",
        filter_expression="'Table'[Column]='Value'",
    )
```

### Set OLS
- **Patterns**: object level security, set ols, add ols
- **Function**: tom.set_ols
- **Code**:
```python
from sempy_labs.tom import connect_semantic_model

with connect_semantic_model(dataset={dataset}, readonly=False, workspace={workspace}) as tom:
    tom.set_ols(
        role_name="RoleName",
        table_name="TableName",
        column_name="ColumnName",
        permission="None",
    )
```

### Set Synonym
- **Patterns**: set synonym, add synonym
- **Function**: tom.set_synonym
- **Code**:
```python
from sempy_labs.tom import connect_semantic_model

with connect_semantic_model(dataset={dataset}, readonly=False, workspace={workspace}) as tom:
    tom.set_synonym(
        culture="en-US",
        synonym_name="MySynonym",
        object=tom.model.Tables["TableName"],
    )
```

### User Defined Function
- **Patterns**: user defined function, udf, create function, set function
- **Function**: tom.set_user_defined_function
- **Code**:
```python
from sempy_labs.tom import connect_semantic_model

with connect_semantic_model(dataset={dataset}, readonly=False, workspace={workspace}) as tom:
    tom.set_user_defined_function(
        name="FunctionName",
        expression="(amount : NUMERIC) => amount * 1.1",
    )
```

---

## Direct Lake

### Update Direct Lake Connection
- **Patterns**: direct lake connection, update direct lake, dl connection
- **Function**: update_direct_lake_model_connection

### Update Direct Lake Partition Entity
- **Patterns**: direct lake partition, update partition entity, dl partition
- **Function**: update_direct_lake_partition_entity

### Migrate Direct Lake to Import
- **Patterns**: migrate direct lake, direct lake to import, convert direct lake
- **Function**: migrate_direct_lake_to_import

### Warm Direct Lake Cache
- **Patterns**: warm cache, warm direct lake, dl cache, direct lake cache
- **Function**: warm_direct_lake_cache_isresident

### Set Autosync
- **Patterns**: set autosync, autosync, auto sync
- **Function**: set_autosync

---

## Admin

### Scan Workspaces
- **Patterns**: scan workspace, scanner api, scan workspaces
- **Function**: scan_workspaces

### List Activity Events
- **Patterns**: activity events, list activity, show activity
- **Function**: list_activity_events

### List Tenant Settings
- **Patterns**: tenant settings, list tenant settings, show tenant settings
- **Function**: list_tenant_settings

---

## Lakehouses

### Get Lakehouse Tables
- **Patterns**: lakehouse tables, show lakehouse tables, list lakehouse tables, tables in lakehouse
- **Function**: get_lakehouse_tables

### Get Lakehouse Columns
- **Patterns**: lakehouse columns, show lakehouse columns, columns in lakehouse
- **Function**: get_lakehouse_columns

### List Shortcuts
- **Patterns**: list shortcuts, show shortcuts, shortcuts
- **Function**: list_shortcuts

### Create OneLake Shortcut
- **Patterns**: create shortcut, onelake shortcut, create onelake shortcut
- **Function**: create_shortcut_onelake

### Recover Lakehouse Object
- **Patterns**: recover lakehouse, recover table, recover file
- **Function**: recover_lakehouse_object

### Optimize Lakehouse Tables
- **Patterns**: optimize lakehouse, optimize table, optimize tables
- **Function**: optimize_lakehouse_tables

### Vacuum Lakehouse Tables
- **Patterns**: vacuum lakehouse, vacuum table, vacuum tables
- **Function**: vacuum_lakehouse_tables

---

## Reports

### Rebind Report
- **Patterns**: rebind report, report rebind, bind report
- **Function**: report_rebind

### Report BPA
- **Patterns**: report bpa, report best practice, run report bpa
- **Function**: run_report_bpa

### View Report Metadata
- **Patterns**: report metadata, list pages, list visuals, report pages, report visuals, report filters
- **Function**: report.connect_report
- **Code**:
```python
from sempy_labs.report import connect_report

with connect_report(report={report}, workspace={workspace}) as rpt:
    df = rpt.list_pages()
    # df = rpt.list_visuals()
    # df = rpt.list_custom_visuals()
    # df = rpt.list_report_filters()
    # df = rpt.list_page_filters()
    # df = rpt.list_visual_filters()
    # df = rpt.list_visual_objects()
    # df = rpt.list_bookmarks()
df
```

### Get Report Theme
- **Patterns**: get theme, report theme, get report theme
- **Function**: report.get_theme
- **Code**:
```python
from sempy_labs.report import connect_report

with connect_report(report={report}, workspace={workspace}) as rpt:
    theme = rpt.get_theme()
theme
```

### Set Report Theme
- **Patterns**: set theme, set report theme, update theme
- **Function**: report.set_theme
- **Code**:
```python
from sempy_labs.report import connect_report

with connect_report(report={report}, workspace={workspace}, readonly=False) as rpt:
    rpt.set_theme(theme_file_path="path/to/theme.json")
```

### Set Page Order
- **Patterns**: page order, set page order, reorder pages
- **Function**: report.set_page_order
- **Code**:
```python
from sempy_labs.report import connect_report

with connect_report(report={report}, workspace={workspace}, readonly=False) as rpt:
    rpt.set_page_order(order=["Page 1", "Page 2", "Page 3"])
```

### Save Report as PBIP
- **Patterns**: save as pbip, save report pbip, export pbip, pbip
- **Function**: save_report_as_pbip

### Remove Unnecessary Custom Visuals
- **Patterns**: remove custom visuals, unnecessary custom visuals, clean custom visuals
- **Function**: report.remove_unnecessary_custom_visuals
- **Code**:
```python
from sempy_labs.report import connect_report

with connect_report(report={report}, workspace={workspace}, readonly=False) as rpt:
    rpt.remove_unnecessary_custom_visuals()
```

### Migrate Report Level Measures
- **Patterns**: migrate report measure, report level measure, migrate measures from report
- **Function**: report.migrate_report_level_measures
- **Code**:
```python
from sempy_labs.report import connect_report

with connect_report(report={report}, workspace={workspace}, readonly=False) as rpt:
    rpt.migrate_report_level_measures()
```

### Set Active Page
- **Patterns**: set active page, active page
- **Function**: report.set_active_page
- **Code**:
```python
from sempy_labs.report import connect_report

with connect_report(report={report}, workspace={workspace}, readonly=False) as rpt:
    rpt.set_active_page(page_name="PageName")
```

### Set Page Visibility
- **Patterns**: page visibility, hide page, show page, set page visibility
- **Function**: report.set_page_visibility
- **Code**:
```python
from sempy_labs.report import connect_report

with connect_report(report={report}, workspace={workspace}, readonly=False) as rpt:
    rpt.set_page_visibility(page_name="PageName", hidden=True)
```

### Find Broken Visuals
- **Patterns**: broken visuals, find broken, invalid visuals
- **Function**: report.list_semantic_model_objects
- **Code**:
```python
from sempy_labs.report import ReportWrapper

rpt = ReportWrapper(report={report}, workspace={workspace})
df = rpt.list_semantic_model_objects(extended=True)
df[df["Valid Semantic Model Object"] == False]
```

---

## Monitoring

### Query Workspace Monitoring
- **Patterns**: workspace monitoring, query monitoring, monitor workspace
- **Function**: query_workspace_monitoring

---

## Dataflows

### List Dataflows
- **Patterns**: list dataflows, show dataflows, dataflows
- **Function**: list_dataflows

### Get Dataflow Definition
- **Patterns**: dataflow definition, get dataflow, dataflow code
- **Function**: get_dataflow_definition

### Upgrade Dataflow
- **Patterns**: upgrade dataflow, dataflow gen2, dataflow ci cd
- **Function**: upgrade_dataflow

---

## Misc

### List Connections
- **Patterns**: list connections, show connections
- **Function**: list_connections

### Save as Delta Table
- **Patterns**: save delta table, save as delta, delta table
- **Function**: save_as_delta_table

### Refresh SQL Endpoint
- **Patterns**: refresh sql endpoint, sql endpoint metadata, refresh sql metadata
- **Function**: refresh_sql_endpoint_metadata

### Copy Item
- **Patterns**: copy item, copy report, copy notebook, clone item
- **Function**: copy_item

### Search Notebooks
- **Patterns**: search notebook, search notebooks, find in notebook
- **Function**: search_notebooks

### DAXLib Integration
- **Patterns**: daxlib, dax lib, daxlib package
- **Function**: daxlib.add_package_to_semantic_model

### List User Defined Functions
- **Patterns**: list user defined function, list udf, user defined functions
- **Function**: list_user_defined_functions
