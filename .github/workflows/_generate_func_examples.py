import inspect
import os
import typing
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

link_prefix = "https://semantic-link-labs.readthedocs.io/en/stable/"
tab = '    '
skip_functions = ['connect_semantic_model', '__init__', 'close']

markdown_example = '## Function Examples\n'
# Function Examples
for d, d_alias in dirs.items():
    d_name = d.__name__   
    for attr_name in dir(d):
        attr = getattr(d, attr_name)
        if inspect.isfunction(attr):
            if attr_name not in skip_functions:
                link = f"{link_prefix}{d_name}.html#{d_name}.{attr_name}"
                if d_alias == 'tom':
                    link = f"{link_prefix}sempy_labs.{d_alias}.html#sempy_labs.{d_alias}.{d_name}.{attr_name}"
                sig = inspect.signature(attr)
                markdown_example += f"\n### [{attr_name}]({link})\n```python"
                markdown_example += "\nimport sempy_labs as labs"
                if d_alias == 'tom':
                    markdown_example += "\nfrom sempy_labs.tom import connect_semantic_model"
                    tf = 'True'
                    markdown_example += f"\nwith connect_semantic_model(dataset='', workspace='', readonly={tf}) as tom:"
                elif d_alias != 'labs':
                    markdown_example += f"\nimport {d_name} as {d_alias}"
                func_print = f"{d_alias}.{attr_name}("
                if d_alias == 'tom':
                    markdown_example += f"\n{tab}{func_print}"
                else:
                    markdown_example += f"\n{func_print}"
                params = [param for param_name, param in sig.parameters.items() if param_name not in ['kwargs', 'self']]
                param_count = len(params)
                for param_name, param in sig.parameters.items():
                    is_optional = False
                    if param_name not in ['kwargs', 'self']:
                        param_value = ''
                        if param.default != inspect.Parameter.empty:
                            param_value = param.default
                            is_optional = True
                        elif param_name == 'dataset':
                            param_value = "AdvWorks"
                        elif param_name in ['email_address', 'user_name']:
                            param_value = 'hello@goodbye.com'
                        elif param_name == 'languages':
                            param_value = ['it-IT', 'zh-CN']
                        elif param_name == 'dax_query':
                            param_value = 'EVALUATE SUMMARIZECOLUMNS("MyMeasure", 1)'
                        elif param_name == 'column':
                            param_value = 'tom.model.Tables["Geography"].Columns["GeographyKey"]'
                        elif param_name in ['object']:
                            if attr_name in ['row_count', 'total_size', 'used_in_relationships', 'used_in_rls', 'set_translation']:
                                param_value = 'tom.model.Tables["Sales"]'
                            elif attr_name in ['records_per_segment']:
                                param_value = 'tom.model.Tables["Sales"].Partitions["Sales"]'
                            elif attr_name in ['used_size']:
                                param_value = 'tom.model.Tables["Geography"].Hierarchies["Geo Hierarchy"]'
                            elif attr_name in ['fully_qualified_measures']:
                                param_value = 'tom.model.Tables["Sales"].Measures["Sales Amount"]'
                        elif param_name == 'dependencies':
                            param_value = 'labs.get_model_calc_dependencies(dataset=tom._dataset, workspace=tom._workspace)'

                        if param_value not in [None, True, False] and not isinstance(param_value, list) and param_name not in ['object', 'column', 'dependencies']:
                            param_value = f"'{param_value}'"
                        p = f"{tab}{param_name}={param_value},"
                        if is_optional:
                            p += " # This parameter is optional"
                        if d_alias == 'tom':
                            markdown_example += f"\n{tab}{p}"
                        else:
                            markdown_example += f"\n{p}"
                closing = ")\n```\n"
                if param_count == 0:
                    markdown_example += closing
                else:
                    markdown_example += f"\n{closing}"

output_path = os.path.join('/root/semantic-link-labs', 'function_examples.md')
with open(output_path, 'w') as f:
    f.write(markdown_example)
