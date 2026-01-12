import re
import io
from uuid import UUID
from typing import Optional, List, Dict, Tuple
import zipfile
from sempy._utils._log import log
from sempy_labs._helper_functions import (
    resolve_workspace_name_and_id,
    resolve_item_name_and_id,
)
from sempy_labs._vpax import download_and_load_nuget_package
from sempy_labs._a_lib_info import (
    lib_name,
    lib_version,
    set_dotnet_runtime,
    nuget_dir,
)
from sempy_labs.tom import connect_semantic_model
import sempy_labs._icons as icons


DAX_LIB_VERSION = "0.5.3-beta"
NUGET_BASE_URL = "https://www.nuget.org/api/v2/package"
ASSEMBLIES = [
    "DaxLib.Client",
]

_dax_lib_initialized = False


def init_dax_lib():

    global _dax_lib_initialized
    if _dax_lib_initialized:
        return

    set_dotnet_runtime()

    for name in ASSEMBLIES:
        download_and_load_nuget_package(
            name, DAX_LIB_VERSION, nuget_dir, load_assembly=True
        )

    _dax_lib_initialized = True


@log
def get_dax_lib_package_and_version(
    package_name: str, version: str = None
) -> Tuple[str, str]:

    init_dax_lib()

    from DaxLib.Client import DaxLibClient, ProductInformation
    from DaxLib.Packaging import PackageId
    from System.Threading import CancellationToken

    package_id = PackageId(package_name)
    token = CancellationToken.get_None()

    client = DaxLibClient(ProductInformation(lib_name, lib_version))

    # -----------------------------
    # Get available versions
    # -----------------------------
    versions = list(client.GetPackageVersionsAsync(package_id, token).Result)
    if not versions:
        raise ValueError(f"No versions found for package '{package_name}'")

    # Map string -> PackageVersion
    version_map = {str(v): v for v in versions}

    if version is None:
        # Pick latest by string comparison (DaxLib already sorts semantically)
        selected_version = versions[0]
    else:
        if version not in version_map:
            raise ValueError(
                f"Version '{version}' not found for package '{package_name}'. "
                f"Available versions: {', '.join(version_map.keys())}"
            )
        selected_version = version_map[version]

    # -----------------------------
    # Download NuGet package
    # -----------------------------
    stream = client.DownloadPackageAsync(
        package_id,
        selected_version,  # ← CORRECT TYPE
        token,
    ).Result

    try:
        package_bytes = bytes(stream.ToArray())
    finally:
        stream.Close()

    # -----------------------------
    # Read ALL contents in-memory
    # -----------------------------
    files: dict[str, bytes] = {}

    with zipfile.ZipFile(io.BytesIO(package_bytes), "r") as z:
        for info in z.infolist():
            if info.is_dir():
                continue
            with z.open(info) as f:
                files[info.filename] = f.read()

    tmdl_bytes = files.get("lib/functions.tmdl")
    tmdl = tmdl_bytes.decode("utf-8")

    return (tmdl, str(selected_version))


@log
def get_dax_lib_package(package_name: str, version: str = None) -> str:
    """
    Gets the TMDL of a DAXLib.org package.

    Parameters
    ----------
    package_name : str
        The name of the DAXLib.org package.
    version : str, default=None
        The version of the package. If None, the latest version is used.

    Returns
    -------
    str
        The TMDL content of the package.
    """
    tmdl, _ = get_dax_lib_package_and_version(
        package_name=package_name, version=version
    )
    return tmdl


FUNC_RE = re.compile(
    r"function\s+'(?P<name>[^']+)'\s*=\s*(?P<body>.*?)(?=^function\s+'|$(?![\r\n]))",
    re.DOTALL | re.MULTILINE,
)


def extract_functions_list(text: str) -> List[Dict[str, str]]:
    """
    Extract functions from text in the format:
      function 'FunctionName' = ...
    Returns a list of dicts with 'name' and 'definition',
    preserving everything after '=' (including ``` if present).
    """
    functions = []
    for m in FUNC_RE.finditer(text):
        functions.append(
            {"name": m.group("name"), "definition": m.group("body").strip()}
        )
    return functions


@log
def add_dax_lib_package(
    dataset: str | UUID,
    package_name: str,
    version: str = None,
    workspace: Optional[str | UUID] = None,
):
    """
    Adds functions from a DAXLib.org package to a semantic model.

    Parameters
    ----------
    dataset : str | uuid.UUID
        The name or ID of the semantic model (dataset).
    package_name : str
        The name of the DAXLib.org package.
    version : str, default=None
        The version of the package. If None, the latest version is used.
    workspace : str | uuid.UUID, default=None
        The workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """
    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    (dataset_name, dataset_id) = resolve_item_name_and_id(
        item=dataset, type="SemanticModel", workspace=workspace_id
    )
    (tmdl, package_version) = get_dax_lib_package_and_version(
        package_name=package_name, version=version
    )
    functions_to_add = extract_functions_list(text=tmdl)

    for f in functions_to_add:
        name = f.get("name")
        expr = f.get("definition")

        with connect_semantic_model(
            dataset=dataset, workspace=workspace, readonly=False
        ) as tom:
            tom.set_user_defined_function(name=name, expression=expr)

    print(
        f"{icons.green_dot} The '{dataset_name}' semantic model within the '{workspace_name}' workspace has been updated to include the function(s) within the '{package_name}' DAXLib.org package (version '{package_version}')."
    )
