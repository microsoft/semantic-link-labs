import sempy
import sempy.fabric as fabric
from typing import List, Optional, Union

green_dot = '\U0001F7E2'
yellow_dot = '\U0001F7E1'
red_dot = '\U0001F534'
in_progress = '⌛'

def create_blank_semantic_model(dataset: str, compatibility_level: Optional[int] = 1605, workspace: Optional[str] = None):
  
  """
    Creates a new blank semantic model (no tables/columns etc.).

    Parameters
    ----------
    dataset : str
        Name of the semantic model.
    compatibility_level : int
        The compatibility level of the semantic model.
        Defaults to 1605.
    workspace : str, default=None
        The Fabric workspace name.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    
    Returns
    -------
    
    """

  if workspace == None:
    workspace_id = fabric.get_workspace_id()
    workspace = fabric.resolve_workspace_name(workspace_id)

  if compatibility_level < 1500:
    print(f"{red_dot} Compatiblity level must be at least 1500.")
    return

  tmsl = f'''
  {{
    "createOrReplace": {{
      "object": {{
        "database": '{dataset}'
      }},
      "database": {{
        "name": '{dataset}',
        "compatibilityLevel": {compatibility_level},
        "model": {{
          "culture": "en-US",
          "defaultPowerBIDataSourceVersion": "powerBI_V3"
        }}
      }}
    }}
  }}
  '''

  fabric.execute_tmsl(script = tmsl, workspace = workspace)

  return print(f"{green_dot} The '{dataset}' semantic model was created within the '{workspace}' workspace.")