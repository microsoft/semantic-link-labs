import inspect
import os
from collections import defaultdict
import sempy_labs
import sempy_labs.migration
import sempy_labs.report
import sempy_labs.directlake
from sempy_labs.tom import TOMWrapper
import sempy_labs.lakehouse

dirs = {
    sempy_labs: 'labs',
    sempy_labs.directlake: 'directlake',
    sempy_labs.lakehouse: 'lake',
    sempy_labs.migration: 'migration',
    sempy_labs.report: 'rep',
    TOMWrapper: 'tom',
}

markdown_content = """
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



"""


link_prefix = "https://semantic-link-labs.readthedocs.io/en/stable/"
skip_functions = ['connect_semantic_model', '__init__', 'close']
functions = {}
for d, d_alias in dirs.items():
    d_name = d.__name__   
    for attr_name in dir(d):
        attr = getattr(d, attr_name)
        if inspect.isfunction(attr):
            if attr_name not in skip_functions:
                category = 'General'
                if d_alias == 'migration':
                    category = 'Direct Lake Migration'
                elif d_alias == 'rep':
                    category = 'Report'
                elif d_alias == 'tom':
                    category = 'Tabular Object Model (TOM)'
                elif d_alias == 'directlake' or 'direct' in attr_name:
                    category = 'Direct Lake'
                elif '_git' in attr_name:
                    category = 'Git'
                elif 'deployment_pipeline' in attr_name:
                    category = 'Deployment Pipelines'
                elif d_alias == 'lake' or 'shortcut' in attr_name or 'lake' in attr_name:
                    category = 'Lakehouse'
                elif 'qso' in attr_name:
                    category = 'Query Scale Out'
                elif attr_name.startswith('resolve_') or attr_name in ['save_as_delta_table', 'create_abfss_path', 'create_relationship_name', 'generate_embedded_filter', 'format_dax_object_name', 'get_capacity_name', 'get_capacity_id']:
                    category = 'Helper Functions'
                elif 'semantic' in attr_name or 'measure_depend' in attr_name or 'dataset' in attr_name or 'evaluate_dax' in attr_name or attr_name in ['clear_cache', 'get_model_calc_dependencies', 'get_object_level_security']:
                    category = 'Semantic Model'
                elif 'bpa' in attr_name or 'vertipaq' in attr_name:
                    category = 'Model Optimization'
                elif 'assign' in attr_name or 'provision' in attr_name or ('user' in attr_name and 'workspace' in attr_name) or attr_name in ['set_workspace_default_storage_format']:
                    category = 'Admin'
                elif 'pool' in attr_name or 'spark' in attr_name:
                    category = 'Spark'
                functions[attr_name] = category

functions = dict(sorted(functions.items(), key=lambda item: (item[1], item[0])))

category_to_funcs = defaultdict(list)
tom_cat = 'Tabular Object Model (TOM)'
category_to_funcs[tom_cat] = defaultdict(list)
for func, category in functions.items():
    if 'TOM' not in category:        
        category_to_funcs[category].append(func)
    else:
        if 'vertipaq' in func or func in ['row_count', 'used_size', 'data_size', 'dictionary_size', 'total_size', 'cardinality', 'records_per_segment']:
            category_to_funcs[tom_cat]["Vertipaq Stats"].append(func) 
        elif 'policy' in func or 'incremental' in func:
            category_to_funcs[tom_cat]["Incremental Refresh"].append(func)
        elif 'annotation' in func:
            category_to_funcs[tom_cat]["Annotations"].append(func)
        elif 'extended_prop' in func:
            category_to_funcs[tom_cat]["Extended Properties"].append(func)
        elif 'perspective' in func:
            category_to_funcs[tom_cat]["Perspectives"].append(func)
        elif 'translat' in func:
            category_to_funcs[tom_cat]["Translations"].append(func)
        elif func.startswith('all_'):
            category_to_funcs[tom_cat]["'All' functions"].append(func)
        elif func.startswith('add_'):
            category_to_funcs[tom_cat]["'Add' functions"].append(func)
        elif func.startswith('set_'):
            category_to_funcs[tom_cat]["'Set' functions"].append(func)
        elif func.startswith('update_'):
            category_to_funcs[tom_cat]["'Update' functions"].append(func)
        elif func.startswith('remove_'):
            category_to_funcs[tom_cat]["'Remove' functions"].append(func)
        elif func.startswith('used_in_') or func in ['depends_on', 'referenced_by', 'fully_qualified_measures', 'unqualified_columns']:
            category_to_funcs[tom_cat]["'Used-in' and dependency functions"].append(func)
        else:
            category_to_funcs[tom_cat]["Misc functions"].append(func)

sorted_category_to_funcs = {}
for category in sorted(category_to_funcs.keys()):
    if isinstance(category_to_funcs[category], defaultdict):
        sorted_subcategories = {}
        for subcategory in sorted(category_to_funcs[category].keys()):
            sorted_subcategories[subcategory] = sorted(category_to_funcs[category][subcategory])
        sorted_category_to_funcs[category] = sorted_subcategories
    else:
        sorted_category_to_funcs[category] = sorted(category_to_funcs[category])

markdown_content += '## Function Categories\n'
for category, funcs in sorted_category_to_funcs.items():
    if 'TOM' in category:
        markdown_content += "\n### [Tabular Object Model](https://learn.microsoft.com/analysis-services/tom/introduction-to-the-tabular-object-model-tom-in-analysis-services-amo?view=asallproducts-allversions) ([TOM](https://learn.microsoft.com/dotnet/api/microsoft.analysisservices.tabular.model?view=analysisservices-dotnet))"
        for sub_category, tom_list in funcs.items():
            markdown_content += f"\n#### {sub_category}"
            for tom_func in tom_list:
                markdown_content += f"\n* [{tom_func}](https://github.com/microsoft/semantic-link-labs)"
            markdown_content += '\n'
        markdown_content += '\n'
    else:
        markdown_content += f"\n### {category}"
        for func in funcs:
            markdown_content += f"\n* [{func}](https://github.com/microsoft/semantic-link-labs)"
        markdown_content += '\n'

markdown_content += """
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
"""

output_path = os.path.join('/root/semantic-link-labs', 'README.md')
with open(output_path, 'w') as f:
    f.write(markdown_content)
