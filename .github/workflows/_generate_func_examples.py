import inspect
import re
from docstring_parser import parse
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

# Data type mapping
data_type_link_prefix = ""
data_type_map = {
    "str": "https://docs.python.org/3/library/stdtypes.html#str",
    "list": "https://docs.python.org/3/library/stdtypes.html#list",
    "bool": "https://docs.python.org/3/library/stdtypes.html#bool",
    "dict": "https://docs.python.org/3/library/typing.html#typing.Dict",
    "pandas.DataFrame": "http://pandas.pydata.org/pandas-docs/dev/reference/api/pandas.DataFrame.html#pandas.DataFrame",
}

data_types = list(data_type_map.keys())
pattern_type = r'(' + '|'.join(re.escape(dt) for dt in data_types) + r')'


def replace_data_type(match):
    data_type = match.group(1)  # Extract the matched data type
    if data_type in data_type_map:
        # Build the full link
        return f'[{data_type}]({data_type_map[data_type]})'
    return match.group(0)  # If no match, return the original string


link_prefix = "https://semantic-link-labs.readthedocs.io/en/stable/"
tab = '    '
skip_functions = ['connect_semantic_model', '__init__', 'close']
pattern_desc = r'`([A-Za-z ]+) <(https?://[^\s]+)>`_'
default_values = {
    'dataset': "AdvWorks",
    'email_address': 'hello@goodbye.com',
    'user_name': 'hello@goodbye.com',
    'languages': ['it-IT', 'zh-CN'],
    'dax_query': 'EVALUATE SUMMARIZECOLUMNS("MyMeasure", 1)',
    'column': 'tom.model.Tables["Geography"].Columns["GeographyKey"]',
    'dependencies': 'labs.get_model_calc_dependencies(dataset=tom._dataset, workspace=tom._workspace)',
}


def format_link(d_alias, d_name, attr_name):
    return f"{link_prefix}sempy_labs.{d_alias}.html#sempy_labs.{d_alias}.{d_name}.{attr_name}" if d_alias == 'tom' else f"{link_prefix}{d_name}.html#{d_name}.{attr_name}"


def create_signature(attr_name, sig, d_alias):
    func_print = f"{d_alias}.{attr_name}("
    params = [
        f"{param_name}={default_values.get(param_name, param.default) if param.default != inspect.Parameter.empty else ''}"
        for param_name, param in sig.parameters.items() if param_name not in ['kwargs', 'self']
    ]
    return func_print + ', '.join(params) + ")"


def format_docstring_description(description):
    return re.sub(pattern_desc, r'[\1](\2)', str(description))


markdown_example = '## Function Examples\n'

# Gather necessary ingredients into a dictionary
func_dict = {}
for d, d_alias in dirs.items():
    d_name = d.__name__
    for attr_name in dir(d):
        attr = getattr(d, attr_name)
        if inspect.isfunction(attr) and attr_name not in skip_functions:
            func_dict[attr_name] = {
                'attr': attr,
                'directory': d_name,
                'directory_alias': d_alias,
            }

for attr_name, attr_info in func_dict.items():
    attr = attr_info['attr']
    d_name = attr_info['directory']
    d_alias = attr_info['directory_alias']

    docstring = parse(attr.__doc__)
    sig = inspect.signature(attr)
    link = format_link(d_alias, d_name, attr_name)
    description = format_docstring_description(docstring.description)

    # Add Function name with link and description
    markdown_example += f"\n### [{attr_name}]({link})\n#### {description}"
    # Add Example Section
    markdown_example += "\n```python\nimport sempy_labs as labs"

    if d_alias == 'tom':
        markdown_example += "\nfrom sempy_labs.tom import connect_semantic_model\nwith connect_semantic_model(dataset='', workspace='', readonly=True) as tom:"

    markdown_example += f"\n{create_signature(attr_name, sig, d_alias)}\n```\n"

    # Add Parameters Section
    if docstring.params:
        markdown_example += "\n### Parameters"
        for param_name, p in sig.parameters.items():
            ind = str(p).find(':')+1
            p_type = str(p)[ind:].lstrip()
            param_type = re.sub(pattern_type, replace_data_type, p_type)

            req = 'Optional' if p.default != inspect.Parameter.empty else 'Required'
            p_description = next((param.description for param in docstring.params if param.arg_name == param_name), None)
            p_description = format_docstring_description(p_description)

            markdown_example += f"\n> **{param_name}** ({param_type})\n>\n>> {req}; {p_description}\n>"

    # Add Returns Section
    if docstring.returns:
        ret = docstring.returns
        ret_type = ret.type_name
        return_type = re.sub(pattern_type, replace_data_type, ret_type)

        markdown_example += f"\n### Returns\n> {return_type}; {ret.description}"

# Write to file
output_path = '/root/semantic-link-labs/function_examples.md'
with open(output_path, 'w') as f:
    f.write(markdown_example)
