import os
import sys
from pathlib import Path
import sempy
import sempy.fabric
from sempy.fabric._client import DatasetXmlaClient
import tempfile
from uuid import uuid4


_dotnet_tools_initialized = False


def _init_dotnet_tools() -> None:
    global _dotnet_tools_initialized
    if _dotnet_tools_initialized:
        return

    sempy.fabric._client._utils._init_analysis_services()

    my_path = Path(__file__).parent

    import clr

    assembly_path = my_path / "lib"

    sys.path.append(os.fspath(assembly_path))
    clr.AddReference(os.fspath(assembly_path / "Dax.Metadata.dll"))
    clr.AddReference(
        os.fspath(assembly_path / "Microsoft.Fabric.SemanticLinkLabs.Tools.dll")
    )

    _dotnet_tools_initialized = True


def vpax_analyzer(
    workspace: str,
    dataset: str,
    exclude_vpa: bool = False,
    exclude_tom: bool = False,
    analyseDirectQuery: bool = False,
    analyseDirectLake: str = "Full",
) -> None:
    _init_dotnet_tools()

    import System
    from Microsoft.Fabric.SemanticLinkLabs import VpaxTools
    from Dax.Metadata import DirectLakeExtractionMode

    tom_server = sempy.fabric.create_tom_server(workspace=workspace)

    workspace_id = sempy.fabric.resolve_workspace_id(workspace)
    workspace_name = sempy.fabric.resolve_workspace_name(workspace_id)

    client = DatasetXmlaClient(workspace_id, dataset)

    def get_database(tom_server, name):
        for d in tom_server.Databases:
            if d.Name == name:
                return d

        raise ValueError(f"Unable to find database {name}")

    # Manually build file temp_file_name. Using a simpler tempfile.NamedTemporaryFile breaks on Windows,
    # where file held by python cannot be overriden by C#
    temp_file_name = os.path.join(
        tempfile.gettempdir(), f"vpax-analyzer-{uuid4()}.vpax"
    )

    try:
        with client.get_adomd_connection() as conn:
            VpaxTools.Export(
                get_database(tom_server, dataset).Model,
                conn,
                workspace_name,
                dataset,
                exclude_vpa,
                exclude_tom,
                analyseDirectQuery,
                System.Enum.Parse(DirectLakeExtractionMode, analyseDirectLake),
                temp_file_name,
            )

        # TODO: add parsing
        print(temp_file_name)
    finally:
        pass

        # TODO: enable once the parsing is done

        # try:
        #     os.remove(temp_file_name)
        # except FileNotFoundError:
        #     # File will not be written if exception thrown in DAXToParquet (e.g. error in DAX)
        #     pass
