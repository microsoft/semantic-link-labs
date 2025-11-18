# Semantic Link Labs

[![PyPI version](https://badge.fury.io/py/semantic-link-labs.svg)](https://badge.fury.io/py/semantic-link-labs)
[![Read The Docs](https://readthedocs.org/projects/semantic-link-labs/badge/?version=0.12.7&style=flat)](https://readthedocs.org/projects/semantic-link-labs/)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![Downloads](https://static.pepy.tech/badge/semantic-link-labs)](https://pepy.tech/project/semantic-link-labs)

---
[Read the documentation on ReadTheDocs!](https://semantic-link-labs.readthedocs.io/en/stable/)
---

[Read the Wiki!](https://github.com/microsoft/semantic-link-labs/wiki)
---

[See code examples!](https://github.com/microsoft/semantic-link-labs/wiki/Code-Examples)
---

Semantic Link Labs is a Python library designed for use in [Microsoft Fabric notebooks](https://learn.microsoft.com/fabric/data-engineering/how-to-use-notebook). This library extends the capabilities of [Semantic Link](https://learn.microsoft.com/fabric/data-science/semantic-link-overview) offering additional functionalities to seamlessly integrate and work alongside it. The goal of Semantic Link Labs is to simplify technical processes, empowering people to focus on higher level activities and allowing tasks that are better suited for machines to be efficiently handled without human intervention.

If you encounter any issues, please [raise a bug](https://github.com/microsoft/semantic-link-labs/issues/new?assignees=&labels=&projects=&template=bug_report.md&title=).

If you have ideas for new features/functions, please [request a feature](https://github.com/microsoft/semantic-link-labs/issues/new?assignees=&labels=&projects=&template=feature_request.md&title=).

Check out the video below for an introduction to Semantic Link, Semantic Link Labs and demos of key features!

[![Semantic Link Labs Video](https://img.youtube.com/vi/LSoWDEZk9b0/0.jpg)](https://www.youtube.com/watch?v=LSoWDEZk9b0)

## Featured Scenarios
* Semantic Models
    * [Migrating an import/DirectQuery semantic model to Direct Lake](https://github.com/microsoft/semantic-link-labs?tab=readme-ov-file#direct-lake-migration)
    * [Model Best Practice Analyzer (BPA)](https://github.com/microsoft/semantic-link-labs/wiki/Code-Examples#model-best-practice-analyzer)
    * [Vertipaq Analyzer](https://github.com/microsoft/semantic-link-labs/wiki/Code-Examples#vertipaq-analyzer)
    * [Create a .vpax file](https://github.com/microsoft/semantic-link-labs/wiki/Code-Examples#create-a-vpax-file)
    * [Tabular Object Model](https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Tabular%20Object%20Model.ipynb) [(TOM)](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html)
    * [Translate a semantic model's metadata](https://github.com/microsoft/semantic-link-labs/wiki/Code-Examples#translate-a-semantic-model)
    * [Check Direct Lake Guardrails](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.lakehouse.html#sempy_labs.lakehouse.get_lakehouse_tables)
    * [Refresh](https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Semantic%20Model%20Refresh.ipynb), [clear cache](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.clear_cache), [backup](https://github.com/microsoft/semantic-link-labs/wiki/Code-Examples#backup-a-semantic-model), [restore](https://github.com/microsoft/semantic-link-labs/wiki/Code-Examples#restore-a-semantic-model), [copy backup files](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.copy_semantic_model_backup_file), [move/deploy across workspaces](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.deploy_semantic_model)
    * [Run DAX queries which impersonate a user](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.evaluate_dax_impersonation)
    * [Manage Query Scale Out](https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Query%20Scale%20Out.ipynb)
    * [Auto-generate descriptions for any/all measures in bulk](https://github.com/microsoft/semantic-link-labs/wiki/Code-Examples#auto-generate-measure-descriptions)
    * [Warm the cache of a Direct Lake semantic model after a refresh (using columns currently in memory)](https://github.com/microsoft/semantic-link-labs/wiki/Code-Examples#warm-cache-the-cache-of-a-direct-lake-semantic-model)
    * [Warm the cache of a Direct Lake semantic model (via perspective)](https://github.com/microsoft/semantic-link-labs/wiki/Code-Examples#warm-cache-the-cache-of-a-direct-lake-semantic-model)
    * [Visualize a refresh](https://github.com/microsoft/semantic-link-labs/wiki/Code-Examples#refresh-a-semantic-model)
    * [Update the connection of a Direct Lake semantic model](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.directlake.html#sempy_labs.directlake.update_direct_lake_model_connection)
    * [Dynamically generate a Direct Lake semantic model](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.directlake.html#sempy_labs.directlake.generate_direct_lake_semantic_model)
    * [Check why a Direct Lake semantic model would fallback to DirectQuery](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.directlake.html#sempy_labs.directlake.check_fallback_reason)
    * [View a measure dependency tree](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.measure_dependency_tree)
    * [View unique columns touched in a single (or multiple) DAX query(ies)](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.get_dax_query_dependencies)
    * [Analyze delta tables for Direct Lake semantic models using Delta Analyzer](https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Delta%20Analyzer.ipynb)
    * [View synonyms from the linguistic schema](https://github.com/microsoft/semantic-link-labs/wiki/Code-Examples#list-the-synonyms-in-the-linguistic-metadata)
    * [Add](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.add_incremental_refresh_policy), [update](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.update_incremental_refresh_policy) and [view](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.TOMWrapper.show_incremental_refresh_policy) an incremental refresh policy.
* Reports
    * [Report Best Practice Analyzer (BPA)](https://github.com/microsoft/semantic-link-labs/wiki/Code-Examples#report-best-practice-analyzer)
    * [View report metadata](https://github.com/microsoft/semantic-link-labs/wiki/Code-Examples#view-report-metadata)
    * [View semantic model objects most frequently used in Power BI reports](https://github.com/microsoft/semantic-link-labs/wiki/Code-Examples#show-the-frequency-of-semantic-model-object-used-within-reports)
    * [View broken reports](https://github.com/microsoft/semantic-link-labs/wiki/Code-Examples#find-broken-visuals-in-a-power-bi-report)
    * [Set a report theme](https://github.com/microsoft/semantic-link-labs/wiki/Code-Examples#set-the-theme-of-a-report)
    * [Migrate report-level measures to the semantic model](https://github.com/microsoft/semantic-link-labs/wiki/Code-Examples#migrate-report-level-measures-to-the-semantic-model)
    * [Rebind reports](https://github.com/microsoft/semantic-link-labs/wiki/Code-Examples#rebind-a-report-to-a-different-semantic-model)
    * [Save a report as a .pbip](https://github.com/microsoft/semantic-link-labs/wiki/Code-Examples#save-a-report-as-a-pbip-file)
* Capacities
    * [Migrating a Power BI Premium capacity (P sku) to a Fabric capacity (F sku)](https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Capacity%20Migration.ipynb)
    * [Migrating a Fabric Trial capacity (FT sku) to a Fabric capacity (F sku)](https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Capacity%20Migration.ipynb)
    * [Create](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.create_fabric_capacity)/[update](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.update_fabric_capacity)/[suspend](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.suspend_fabric_capacity)/[resume](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.resume_fabric_capacity) Fabric capacities
* Lakehouses
    * [Optimize lakehouse tables](https://github.com/microsoft/semantic-link-labs/wiki/Code-Examples#optimize-lakehouse-tables)
    * [Vacuum lakehouse tables](https://github.com/microsoft/semantic-link-labs/wiki/Code-Examples#vacuum-lakehouse-tables)
    * [Create](https://github.com/microsoft/semantic-link-labs/wiki/Code-Examples#create-a-onelake-shortcut), [delete](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.lakehouse.html#sempy_labs.lakehouse.delete_shortcut), and [view shortcuts](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.list_shortcuts)
    * [Analyze delta tables for Direct Lake semantic models using Delta Analyzer](https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Delta%20Analyzer.ipynb)
    * [Recover a soft-deleted lakehouse table/file/folder](https://github.com/microsoft/semantic-link-labs/wiki/Code-Examples#recover-a-lakehouse-object)
* Notebooks
    * [Import a notebook from the web](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.import_notebook_from_web)    
* APIs
    * Wrapper functions for [Power BI](https://learn.microsoft.com/rest/api/power-bi/), [Fabric](https://learn.microsoft.com/rest/api/fabric/articles/using-fabric-apis), [Azure](https://learn.microsoft.com/rest/api/microsoftfabric/fabric-capacities?view=rest-microsoftfabric-2023-11-01), and [Microsoft Graph](https://learn.microsoft.com/graph/api/overview?view=graph-rest-1.0) APIs
* Service Principal Authentication
    * Now supported using the [service_principal_authentication](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.service_principal_authentication) context manager for the [admin](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.admin.html) subpackage, Azure API wrapper functions, and [connect_semantic_model](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.tom.html#sempy_labs.tom.connect_semantic_model). See this [helper notebook](https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Service%20Principal.ipynb) for additional instructions.

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
import sempy_labs.lakehouse as lake
import sempy_labs.report as rep
from sempy_labs import admin, deployment_pipeline, directlake, eventstream, graph, migration, mirrored_azure_databricks_catalog, ml_model, theme, variable_library
from sempy_labs.tom import connect_semantic_model
from sempy_labs.report import connect_report
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
* [0.12.7](https://github.com/microsoft/semantic-link-labs/releases/tag/0.12.7) (November 19, 2025)
* [0.12.6](https://github.com/microsoft/semantic-link-labs/releases/tag/0.12.6) (October 30, 2025)
* [0.12.5](https://github.com/microsoft/semantic-link-labs/releases/tag/0.12.5) (October 30, 2025)
* [0.12.4](https://github.com/microsoft/semantic-link-labs/releases/tag/0.12.4) (October 16, 2025)
* [0.12.3](https://github.com/microsoft/semantic-link-labs/releases/tag/0.12.3) (September 17, 2025)
* [0.12.2](https://github.com/microsoft/semantic-link-labs/releases/tag/0.12.2) (September 12, 2025)
* [0.12.1](https://github.com/microsoft/semantic-link-labs/releases/tag/0.12.1) (September 4, 2025)
* [0.12.0](https://github.com/microsoft/semantic-link-labs/releases/tag/0.12.0) (September 2, 2025)
* [0.11.3](https://github.com/microsoft/semantic-link-labs/releases/tag/0.11.3) (August 6, 2025)
* [0.11.2](https://github.com/microsoft/semantic-link-labs/releases/tag/0.11.2) (July 8, 2025)
* [0.11.1](https://github.com/microsoft/semantic-link-labs/releases/tag/0.11.1) (June 24, 2025)
* [0.11.0](https://github.com/microsoft/semantic-link-labs/releases/tag/0.11.0) (June 23, 2025)
* [0.10.1](https://github.com/microsoft/semantic-link-labs/releases/tag/0.10.1) (June 10, 2025)
* [0.10.0](https://github.com/microsoft/semantic-link-labs/releases/tag/0.10.0) (May 30, 2025)
* [0.9.11](https://github.com/microsoft/semantic-link-labs/releases/tag/0.9.11) (May 22, 2025)
* [0.9.10](https://github.com/microsoft/semantic-link-labs/releases/tag/0.9.10) (April 24, 2025)
* [0.9.9](https://github.com/microsoft/semantic-link-labs/releases/tag/0.9.9) (April 7, 2025)
* [0.9.8](https://github.com/microsoft/semantic-link-labs/releases/tag/0.9.8) (April 3, 2025)
* [0.9.7](https://github.com/microsoft/semantic-link-labs/releases/tag/0.9.7) (April 1, 2025)
* [0.9.6](https://github.com/microsoft/semantic-link-labs/releases/tag/0.9.6) (March 12, 2025)
* [0.9.5](https://github.com/microsoft/semantic-link-labs/releases/tag/0.9.5) (March 7, 2025)
* [0.9.4](https://github.com/microsoft/semantic-link-labs/releases/tag/0.9.4) (February 27, 2025)
* [0.9.3](https://github.com/microsoft/semantic-link-labs/releases/tag/0.9.3) (February 13, 2025)
* [0.9.2](https://github.com/microsoft/semantic-link-labs/releases/tag/0.9.2) (February 5, 2025)
* [0.9.1](https://github.com/microsoft/semantic-link-labs/releases/tag/0.9.1) (January 22, 2025)
* [0.9.0](https://github.com/microsoft/semantic-link-labs/releases/tag/0.9.0) (January 22, 2025)
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
2. Make sure you are in the 'Fabric Developer' experience. Click the icon at the bottom left corner of your Workspace screen and select 'Fabric'.
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

## How to Contribute

#### Initial setup
1. Fork the Semantic Link Labs GitHub repository (Fork -> Create a new fork).
2. Click on the green icon '<> Code' and copy the 'HTTPS' URL to the clipboard.
3. Open Visual Studio Code.
4. Select 'Clone Git Repository'.
5. Paste the URL copied in step 2.
6. Select 'Clone from URL'.
7. Select/create a folder to map the cloned repository to your computer.
8. When prompted to open the cloned repository, click 'Open'.
9. Open the integrated terminal (View -> Terminal).
10. Run the following code in the terminal to ensure a virtual environment exists.
```python
python -m venv venv
```
11. Run the following code in the terminal to activate the virtual environment.
```python
.\venv\Scripts\Activate.ps1
```
12. Run the following code in the terminal to install the build module.
```
pip install build
```

#### Branching
When making changes, always create a new branch.

1. Navigate to the Souce Control tab within Visual Studio Code.
2. Click on the Source Control icon at the bottom left corner of the screen.
3. Click 'Create new branch...'.
4. Enter the branch name (i.e. yourusername/branchname).
5. Click 'Enter'.

#### Building the .whl file
Running the following in the terminal in Visual Studio Code will create a .whl file in the 'dist' folder within your locally-cloned repository.

```cli
python -m build
```

#### Running and testing the .whl file
1. Open a notebook in a Fabric workspace.
2. Navigate to 'Resources' within the Explorer tab on the left pane. Do not use the 'Files' section.
3. Upload the .whl file to the 'Resource' section.
4. Click on the '...' next to the .whl file and click 'Copy relative path'.
5. Enter '%pip install ""' into a notebook cell. Within the double quotes, paste the copied path from step 4.
6. Run the notebook cell.

#### Submitting a Pull Request (PR)
1. Within the 'Source Control' tab, commit your changes to the branch.
2. Navigate to the [GitHub repo](https://github.com/microsoft/semantic-link-labs).
3. A 'Create pull request' will appear at the top of the screen in green. Click it.
4. Enter details into the description.
5. Click 'Create'.

#### Code Formatting
We use [black](github.com/psf/black) formatting as a code formatting standard. Make sure to run 'black' formatting on your code before submitting a pull request.

Run this code to install black
```cli
pip install black==25.1.0
```

Run this code to format your code using black
```cli
python -m black src
```

## Trademarks

This project may contain trademarks or logos for projects, products, or services. Authorized use of Microsoft 
trademarks or logos is subject to and must follow 
[Microsoft's Trademark & Brand Guidelines](https://www.microsoft.com/en-us/legal/intellectualproperty/trademarks/usage/general).
Use of Microsoft trademarks or logos in modified versions of this project must not cause confusion or imply Microsoft sponsorship.
Any use of third-party trademarks or logos are subject to those third-party's policies.
