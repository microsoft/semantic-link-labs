# Semantic Link Labs

[![PyPI version](https://badge.fury.io/py/semantic-link-labs.svg)](https://badge.fury.io/py/semantic-link-labs)
[![Read The Docs](https://readthedocs.org/projects/semantic-link-labs/badge/?version=0.8.11&style=flat)](https://readthedocs.org/projects/semantic-link-labs/)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![Downloads](https://static.pepy.tech/badge/semantic-link-labs)](https://pepy.tech/project/semantic-link-labs)

---
[Read the documentation on ReadTheDocs!](https://semantic-link-labs.readthedocs.io/en/stable/)
---

Semantic Link Labs is a Python library designed for use in [Microsoft Fabric notebooks](https://learn.microsoft.com/fabric/data-engineering/how-to-use-notebook). This library extends the capabilities of [Semantic Link](https://learn.microsoft.com/fabric/data-science/semantic-link-overview) offering additional functionalities to seamlessly integrate and work alongside it. The goal of Semantic Link Labs is to simplify technical processes, empowering people to focus on higher level activities and allowing tasks that are better suited for machines to be efficiently handled without human intervention.

If you encounter any issues, please [raise a bug](https://github.com/microsoft/semantic-link-labs/issues/new?assignees=&labels=&projects=&template=bug_report.md&title=).

If you have ideas for new features/functions, please [request a feature](https://github.com/microsoft/semantic-link-labs/issues/new?assignees=&labels=&projects=&template=feature_request.md&title=).

Check out the video below for an introduction to Semantic Link, Semantic Link Labs and demos of key features!

[![Semantic Link Labs Video](https://img.youtube.com/vi/LSoWDEZk9b0/0.jpg)](https://www.youtube.com/watch?v=LSoWDEZk9b0)

## Featured Scenarios
* Semantic Models
    * [Migrating an import/DirectQuery semantic model to Direct Lake](https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#direct-lake-migration)
    * [Model Best Practice Analyzer (BPA)](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.run_model_bpa)
    * [Vertipaq Analyzer](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.vertipaq_analyzer)
    * [Tabular Object Model](https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Tabular%20Object%20Model.ipynb) [(TOM)](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html)
    * [Translate a semantic model's metadata](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.translate_semantic_model)
    * [Check Direct Lake Guardrails](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.lakehouse.html#sempy_labs.lakehouse.get_lakehouse_tables)
    * [Refresh](https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Semantic%20Model%20Refresh.ipynb), [clear cache](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.clear_cache), [backup](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.backup_semantic_model), [restore](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.restore_semantic_model), [copy backup files](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.copy_semantic_model_backup_file), [move/deploy across workspaces](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.deploy_semantic_model)
    * [Run DAX queries which impersonate a user](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.evaluate_dax_impersonation)
    * [Manage Query Scale Out](https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Query%20Scale%20Out.ipynb)
    * [Auto-generate descriptions for any/all measures in bulk](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.generate_measure_descriptions)
    * [Warm the cache of a Direct Lake semantic model after a refresh (using columns currently in memory)](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.directlake.html#sempy_labs.directlake.warm_direct_lake_cache_isresident)
    * [Warm the cache of a Direct Lake semantic model (via perspective)](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.directlake.html#sempy_labs.directlake.warm_direct_lake_cache_perspective)
    * [Visualize a refresh](https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Semantic%20Model%20Refresh.ipynb)
    * [Update the connection of a Direct Lake semantic model](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.directlake.html#sempy_labs.directlake.update_direct_lake_model_connection)
    * [Dynamically generate a Direct Lake semantic model](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.directlake.html#sempy_labs.directlake.generate_direct_lake_semantic_model)
    * [Check why a Direct Lake semantic model would fallback to DirectQuery](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.directlake.html#sempy_labs.directlake.check_fallback_reason)
    * [View a measure dependency tree](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.measure_dependency_tree)
    * [View unique columns touched in a single (or multiple) DAX query(ies)](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.get_dax_query_dependencies)
* Reports
    * [Report Best Practice Analyzer (BPA)](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.report.html#sempy_labs.report.run_report_bpa)
    * [View report metadata](https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Report%20Analysis.ipynb)
    * [View semantic model objects most frequently used in Power BI reports](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.list_semantic_model_object_report_usage)
    * [View broken reports](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.list_report_semantic_model_objects)
    * [Set a report theme](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.report.html#sempy_labs.report.ReportWrapper.set_theme)
    * [Migrate report-level measures to the semantic model](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.report.html#sempy_labs.report.ReportWrapper.migrate_report_level_measures)
    * [Rebind reports](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.report.html#sempy_labs.report.report_rebind)
* Capacities
    * [Migrating a Power BI Premium capacity (P sku) to a Fabric capacity (F sku)](https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Capacity%20Migration.ipynb)
    * [Migrating a Fabric Trial capacity (FT sku) to a Fabric capacity (F sku)](https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Capacity%20Migration.ipynb)
    * [Create](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.create_fabric_capacity)/[update](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.update_fabric_capacity)/[suspend](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.suspend_fabric_capacity)/[resume](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.resume_fabric_capacity) Fabric capacities
* Lakehouses
    * [Optimize lakehouse tables](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.lakehouse.html#sempy_labs.lakehouse.optimize_lakehouse_tables)
    * [Vacuum lakehouse tables](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.lakehouse.html#sempy_labs.lakehouse.vacuum_lakehouse_tables)
    * [Create](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.lakehouse.html#sempy_labs.lakehouse.create_shortcut_onelake), [delete](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.lakehouse.html#sempy_labs.lakehouse.delete_shortcut), and [view shortcuts](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.list_shortcuts)
* Notebooks
    * [Import a notebook from the web](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.import_notebook_from_web)    
* APIs
    * Wrapper functions for [Power BI](https://learn.microsoft.com/rest/api/power-bi/), [Fabric](https://learn.microsoft.com/rest/api/fabric/articles/using-fabric-apis), and [Azure (Fabric Capacity)](https://learn.microsoft.com/rest/api/microsoftfabric/fabric-capacities?view=rest-microsoftfabric-2023-11-01) APIs

## Helper Notebooks

Check out the [helper notebooks](https://github.com/microsoft/semantic-link-labs/tree/main/notebooks) for getting started! 
Run the code below to load all the helper notebooks to the workspace of your choice at once.
```python
import sempy_labs as labs
import requests

workspace_name = None # Update this to the workspace in which you want to save the notebooks
api_url = "https://api.github.com/repos/microsoft/semantic-link-labs/contents/notebooks"
response = requests.get(api_url)
files = response.json()
notebook_files = {file['name'][:-6]: file['html_url'] for file in files if file['name'].endswith('.ipynb')}

for file_name, file_url in notebook_files.items():
    labs.import_notebook_from_web(notebook_name=file_name, url=file_url, workspace=workspace_name)
```

## Install the library in a Fabric notebook
```python
%pip install semantic-link-labs
```

## Once installed, run this code to import the library into your notebook
```python
import sempy_labs as labs
from sempy_labs import migration, directlake, admin
from sempy_labs import lakehouse as lake
from sempy_labs import report as rep
from sempy_labs.tom import connect_semantic_model
from sempy_labs.report import ReportWrapper
from sempy_labs import ConnectWarehouse
from sempy_labs import ConnectLakehouse
```

## Load Semantic Link Labs into a custom [Fabric environment](https://learn.microsoft.com/fabric/data-engineering/create-and-use-environment)
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

## Version History
* [0.8.11](https://github.com/microsoft/semantic-link-labs/releases/tag/0.8.11) (December 19, 2024)
* [0.8.10](https://github.com/microsoft/semantic-link-labs/releases/tag/0.8.10) (December 16, 2024)
* [0.8.9](https://github.com/microsoft/semantic-link-labs/releases/tag/0.8.9) (December 4, 2024)
* [0.8.8](https://github.com/microsoft/semantic-link-labs/releases/tag/0.8.8) (November 28, 2024)
* [0.8.7](https://github.com/microsoft/semantic-link-labs/releases/tag/0.8.7) (November 27, 2024)
* [0.8.6](https://github.com/microsoft/semantic-link-labs/releases/tag/0.8.6) (November 14, 2024)
* [0.8.5](https://github.com/microsoft/semantic-link-labs/releases/tag/0.8.5) (November 13, 2024)
* [0.8.4](https://github.com/microsoft/semantic-link-labs/releases/tag/0.8.4) (October 30, 2024)
* [0.8.3](https://github.com/microsoft/semantic-link-labs/releases/tag/0.8.3) (October 14, 2024)
* [0.8.2](https://github.com/microsoft/semantic-link-labs/releases/tag/0.8.2) (October 2, 2024)
* [0.8.1](https://github.com/microsoft/semantic-link-labs/releases/tag/0.8.1) (October 2, 2024)
* [0.8.0](https://github.com/microsoft/semantic-link-labs/releases/tag/0.8.0) (September 25, 2024)
* [0.7.4](https://github.com/microsoft/semantic-link-labs/releases/tag/0.7.4) (September 16, 2024)
* [0.7.3](https://github.com/microsoft/semantic-link-labs/releases/tag/0.7.3) (September 11, 2024)
* [0.7.2](https://github.com/microsoft/semantic-link-labs/releases/tag/0.7.2) (August 30, 2024)
* [0.7.1](https://github.com/microsoft/semantic-link-labs/releases/tag/0.7.1) (August 29, 2024)
* [0.7.0](https://github.com/microsoft/semantic-link-labs/releases/tag/0.7.0) (August 26, 2024)
* [0.6.0](https://github.com/microsoft/semantic-link-labs/releases/tag/0.6.0) (July 22, 2024)
* [0.5.0](https://github.com/microsoft/semantic-link-labs/releases/tag/0.5.0) (July 2, 2024)
* [0.4.2](https://github.com/microsoft/semantic-link-labs/releases/tag/0.4.2) (June 18, 2024)

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
> Calculated tables are also migrated to Direct Lake (as data tables with their DAX expression stored as model annotations in the new semantic model). Additionally, Field Parameters are migrated as they were in the original semantic model (as a calculated table). [Auto date/time tables](https://learn.microsoft.com/power-bi/guidance/auto-date-time) are not migrated. Auto date/time must be disabled in Power BI Desktop and proper date table(s) must be created prior to migration.

6. Finally, you can easily rebind your all reports which use the import/DQ semantic model to the new Direct Lake semantic model in one click.

### Completing these steps will do the following:
* Offload your Power Query logic to Dataflows Gen2 inside of Fabric (where it can be maintained and development can continue).
* Dataflows Gen2 will create delta tables in your Fabric lakehouse. These tables can then be used for your Direct Lake model.
* Create a new semantic model in Direct Lake mode containing all the standard tables and columns, calculation groups, measures, relationships, hierarchies, roles, row level security, perspectives, and translations from your original semantic model.
* Viable calculated tables are migrated to the new semantic model as data tables. Delta tables are dynamically generated in the lakehouse to support the Direct Lake model. The calculated table DAX logic is stored as model annotations in the new semantic model.
* Field parameters are migrated to the new semantic model as they were in the original semantic model (as calculated tables). Any calculated columns used in field parameters are automatically removed in the new semantic model's field parameter(s).
* Non-supported objects are not transferred (i.e. calculated columns, relationships using columns with unsupported data types etc.).
* Reports used by your original semantic model will be rebinded to your new semantic model.

### Limitations
* Calculated columns are not migrated.
* Auto date/time tables are not migrated.
* References to calculated columns in Field Parameters are removed.
* References to calculated columns in measure expressions or other DAX expressions will break.
* Calculated tables are migrated as possible. The success of this migration depends on the interdependencies and complexity of the calculated table. This part of the migration is a workaround as technically calculated tables are not supported in Direct Lake.
* See [here](https://learn.microsoft.com/fabric/get-started/direct-lake-overview#considerations-and-limitations) for the rest of the limitations of Direct Lake.

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
