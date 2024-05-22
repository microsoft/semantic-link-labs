import sempy
import sempy.fabric as fabric

green_dot = '\U0001F7E2'
yellow_dot = '\U0001F7E1'
red_dot = '\U0001F534'
in_progress = '⌛'

def create_blank_semantic_model(dataset: str, compatibility_level: int = 1605, workspace: str | None = None):

  """
    
    Documentation is available here: https://github.com/m-kovalsky/fabric_cat_tools?tab=readme-ov-file#create_blank_semantic_model

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