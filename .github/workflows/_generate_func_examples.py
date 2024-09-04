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
                for param_name, param in sig.parameters.items():
                    if param_name not in ['kwargs', 'self']:
                        param_value = "''"
                        param_type = param.annotation if param.annotation != inspect._empty else "Unknown"
                        if typing.get_origin(param_type) is typing.Union:
                            args = typing.get_args(param_type)
                            if type(None) in args:
                                param_value = 'None'
                        p = f"{tab}{param_name}={param_value},"
                        if d_alias == 'tom':
                            markdown_example += f"\n{tab}{p}"
                        else:
                            markdown_example += f"\n{p}"
                markdown_example += '\n)\n```\n'

output_path = os.path.join('/root/semantic-link-labs', 'function_examples.md')
with open(output_path, 'w') as f:
    f.write(markdown_example)
