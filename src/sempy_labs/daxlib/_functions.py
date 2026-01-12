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
from sempy_labs._a_lib_info import (
    lib_name,
    lib_version,
    init_dotnet,
)
from sempy_labs.tom import connect_semantic_model
import sempy_labs._icons as icons


@log
def get_daxlib_package_and_version(
    package_name: str, version: str = None
) -> Tuple[str, str]:

    init_dotnet()

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
def get_package(package_name: str, version: str = None) -> str:
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
    tmdl, _ = get_daxlib_package_and_version(package_name=package_name, version=version)
    return tmdl


# Match function name, then everything inside the first pair of ```
FUNC_RE = re.compile(
    r"function\s+'(?P<name>[^']+)'\s*=\s*(```(?P<body>.*?)```)", re.DOTALL
)


def extract_functions_list(text: str) -> List[Dict[str, str]]:
    """
    Extract functions from text in the format:
        function 'FunctionName' = ``` ... ```
    Returns a list of dicts with 'name' and 'definition',
    preserving everything inside the triple backticks and
    stopping before annotations.
    """
    functions = []
    for m in FUNC_RE.finditer(text):
        functions.append(
            {"name": m.group("name"), "definition": m.group("body").strip()}
        )
    return functions


@log
def get_package_functions(
    package_name: str,
    version: str = None,
) -> List[Dict[str, str]]:
    """
    Gets the list of functions from a DAXLib.org package.

    Parameters
    ----------
    package_name : str
        The name of the DAXLib.org package.
    version : str, default=None
        The version of the package. If None, the latest version is used.

    Returns
    -------
    List[Dict[str, str]]
        A list of functions with 'name' and 'definition'.
    """
    tmdl, _ = get_daxlib_package_and_version(package_name=package_name, version=version)
    return extract_functions_list(text=tmdl)


def add_or_remove_package_to_semantic_model(
    dataset: str | UUID,
    package_name: str,
    version: str = None,
    workspace: Optional[str | UUID] = None,
    remove: bool = False,
):

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    (dataset_name, dataset_id) = resolve_item_name_and_id(
        item=dataset, type="SemanticModel", workspace=workspace_id
    )
    (tmdl, package_version) = get_daxlib_package_and_version(
        package_name=package_name, version=version
    )
    funcs = extract_functions_list(text=tmdl)

    # Remove functions within the specified package version
    if remove:
        for f in funcs:
            name = f.get("name")

            with connect_semantic_model(
                dataset=dataset, workspace=workspace, readonly=False
            ) as tom:
                tom.delete_user_defined_function(name=name)
        print(
            f"{icons.green_dot} The '{dataset_name}' semantic model within the '{workspace_name}' workspace has been updated to remove the function(s) from the '{package_name}' DAXLib.org package (version '{package_version}')."
        )
    else:
        # Delete existing functions from this package first
        with connect_semantic_model(
            dataset=dataset, workspace=workspace, readonly=False
        ) as tom:
            for f in tom.model.Functions:
                if f.Name.startswith(f"{package_name}."):
                    tom.delete_user_defined_function(name=f.Name)

            # Add functions from the specified package version
            for fn in funcs:
                function_name = fn.get("name")
                expr = fn.get("definition")

                tom.set_user_defined_function(name=function_name, expression=expr)
                tom.set_annotation(
                    object=tom.model.Functions[function_name],
                    name=package_name,
                    value=package_version,
                )

        print(
            f"{icons.green_dot} The '{dataset_name}' semantic model within the '{workspace_name}' workspace has been updated to include the function(s) within the '{package_name}' DAXLib.org package (version '{package_version}')."
        )


@log
def add_package_to_semantic_model(
    dataset: str | UUID,
    package_name: str,
    version: str = None,
    workspace: Optional[str | UUID] = None,
):
    """
    Adds functions from a DAXLib.org package to a semantic model. First removes any existing functions from the same package (name).

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
    add_or_remove_package_to_semantic_model(
        dataset=dataset,
        package_name=package_name,
        version=version,
        workspace=workspace,
        remove=False,
    )


@log
def remove_package_from_semantic_model(
    dataset: str | UUID,
    package_name: str,
    version: str = None,
    workspace: Optional[str | UUID] = None,
):
    """
    Removes functions from a DAXLib.org package from a semantic model.

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

    add_or_remove_package_to_semantic_model(
        dataset=dataset,
        package_name=package_name,
        version=version,
        workspace=workspace,
        remove=True,
    )
