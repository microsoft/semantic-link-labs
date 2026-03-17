import re
import io
from uuid import UUID
from typing import Optional, List, Dict, Tuple, Literal
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
def get_package_tmdl(package_name: str, version: str = None) -> str:
    """
    Gets the TMDL of a `DAXLib.org <https://www.daxlib.org>`_ package.

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


FUNC_RE = re.compile(
    r"""
    function\s+'(?P<name>[^']+)'\s*=\s*
    (?:
        ```(?P<body_fenced>.*?)```          # fenced body
      |
        (?P<body_unfenced>.*?)              # unfenced body
    )
    (?=
        ^\s*annotation\s+                  # stop before annotations
      | ^\s*function\s+'                   # or next function
      | \Z                                 # or end of file
    )
    """,
    re.DOTALL | re.MULTILINE | re.VERBOSE,
)


def extract_functions_list(text: str) -> List[Dict[str, str]]:
    functions = []

    for m in FUNC_RE.finditer(text):
        body = m.group("body_fenced") or m.group("body_unfenced")

        functions.append(
            {
                "name": m.group("name"),
                "definition": body.rstrip(),
            }
        )

    return functions


@log
def get_package_functions(
    package_name: str,
    version: str = None,
) -> List[Dict[str, str]]:
    """
    Gets the list of functions from a `DAXLib.org <https://www.daxlib.org>`_ package.

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


def add_remove_update_package_to_semantic_model(
    dataset: str | UUID,
    package_name: str,
    version: str = None,
    workspace: Optional[str | UUID] = None,
    method: Literal["add", "remove", "update"] = "add",
):

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    (dataset_name, dataset_id) = resolve_item_name_and_id(
        item=dataset, type="SemanticModel", workspace=workspace_id
    )
    (tmdl, package_version) = get_daxlib_package_and_version(
        package_name=package_name, version=version
    )
    funcs = extract_functions_list(text=tmdl)

    def package_exists(tom, package_name: str) -> bool:
        return any(
            f.Name.lower().startswith(f"{package_name.lower()}.")
            for f in tom.model.Functions
        )

    with connect_semantic_model(
        dataset=dataset, workspace=workspace, readonly=False
    ) as tom:

        exists = package_exists(tom, package_name)

        if method == "remove":
            for fn in funcs:
                tom.delete_user_defined_function(name=fn["name"])

            print(
                f"{icons.green_dot} The '{dataset_name}' semantic model within the "
                f"'{workspace_name}' workspace has been updated to remove the "
                f"function(s) from the '{package_name}' DAXLib.org package "
                f"(version '{package_version}')."
            )
            return

        if method == "add" and exists:
            print(
                f"{icons.info} The '{package_name}' package is already detected in the "
                f"semantic model. If you want to update the package to the latest "
                f"version, please use the 'update_package_in_semantic_model' function."
            )
            return

        if method != "add" and not exists:
            print(
                f"{icons.info} The '{package_name}' package is not detected in the "
                f"semantic model. Please use the 'add_package_to_semantic_model' "
                f"function to add it first."
            )
            return

        # add / update logic (shared); First remove existing functions from the package if they exist
        for f in tom.model.Functions:
            if f.Name.lower().startswith(f"{package_name.lower()}."):
                tom.delete_user_defined_function(name=f.Name)
        for fn in funcs:
            name = fn["name"]
            expr = fn["definition"]

            tom.set_user_defined_function(name=name, expression=expr)
            tom.set_annotation(
                object=tom.model.Functions[name],
                name=package_name,
                value=package_version,
            )

        print(
            f"{icons.green_dot} The '{dataset_name}' semantic model within the "
            f"'{workspace_name}' workspace has been updated to include the "
            f"function(s) within the '{package_name}' DAXLib.org package "
            f"(version '{package_version}')."
        )


@log
def add_package_to_semantic_model(
    dataset: str | UUID,
    package_name: str,
    version: str = None,
    workspace: Optional[str | UUID] = None,
):
    """
    Adds functions from a `DAXLib.org <https://www.daxlib.org>`_ package to a semantic model (if the package is not already present).

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
    add_remove_update_package_to_semantic_model(
        dataset=dataset,
        package_name=package_name,
        version=version,
        workspace=workspace,
        method="add",
    )


@log
def remove_package_from_semantic_model(
    dataset: str | UUID,
    package_name: str,
    version: str = None,
    workspace: Optional[str | UUID] = None,
):
    """
    Removes functions from a `DAXLib.org <https://www.daxlib.org>`_ package from a semantic model.

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

    add_remove_update_package_to_semantic_model(
        dataset=dataset,
        package_name=package_name,
        version=version,
        workspace=workspace,
        method="remove",
    )


@log
def update_package_in_semantic_model(
    dataset: str | UUID,
    package_name: str,
    version: str = None,
    workspace: Optional[str | UUID] = None,
):
    """
    Updates functions from a `DAXLib.org <https://www.daxlib.org>`_ package in a semantic model to a specific version of the package (defaults to latest version).

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

    add_remove_update_package_to_semantic_model(
        dataset=dataset,
        package_name=package_name,
        version=version,
        workspace=workspace,
        method="update",
    )
