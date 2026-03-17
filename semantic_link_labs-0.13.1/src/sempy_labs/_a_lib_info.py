import sempy
import requests
import zipfile
from pathlib import Path

lib_name = "semanticlinklabs"
lib_version = "0.13.1"

NUGET_BASE_URL = "https://www.nuget.org/api/v2/package"
current_dir = Path(__file__).parent
nuget_dir = current_dir / "nuget_dlls"


def find_lib_folder(pkg_folder: Path) -> Path:
    lib_base = pkg_folder / "lib"
    if not lib_base.exists():
        raise FileNotFoundError(f"No 'lib' directory in package {pkg_folder}")

    # Prefer netstandard2.0 if available
    candidates = sorted(lib_base.iterdir())
    for preferred in ["netstandard2.0", "net6.0", "net5.0", "netcoreapp3.1", "net472"]:
        if (lib_base / preferred).exists():
            return lib_base / preferred

    # Fallback: first available folder
    for candidate in candidates:
        if candidate.is_dir():
            return candidate

    raise FileNotFoundError(f"No usable framework folder found in {lib_base}")


def download_and_extract_package(
    package_name: str, version: str, target_dir: Path
) -> Path:
    nupkg_url = f"{NUGET_BASE_URL}/{package_name}/{version}"
    nupkg_path = target_dir / f"{package_name}.{version}.nupkg"

    if not nupkg_path.exists():
        r = requests.get(nupkg_url)
        r.raise_for_status()
        target_dir.mkdir(parents=True, exist_ok=True)
        with open(nupkg_path, "wb") as f:
            f.write(r.content)

    extract_path = target_dir / f"{package_name}_{version}"
    if not extract_path.exists():
        with zipfile.ZipFile(nupkg_path, "r") as zip_ref:
            zip_ref.extractall(extract_path)
    return extract_path


def download_and_load_nuget_package(
    package_name, version, target_dir: Path = None, load_assembly=True
):

    from System.Reflection import Assembly

    if target_dir is None:
        target_dir = nuget_dir

    # Download and extract
    pkg_folder = download_and_extract_package(package_name, version, target_dir)
    lib_folder = find_lib_folder(pkg_folder)

    dll_path = lib_folder / f"{package_name}.dll"
    if not dll_path.exists():
        raise FileNotFoundError(f"{dll_path} not found")

    # sys.path.append(str(lib_folder))
    if load_assembly:
        Assembly.LoadFrom(str(dll_path))  # LoadFile


def is_assembly_loaded(name: str) -> bool:

    import System

    return any(
        asm.GetName().Name == name
        for asm in System.AppDomain.CurrentDomain.GetAssemblies()
    )


_runtime_initialized = False
_nuget_loaded = False


def init_dotnet():
    global _runtime_initialized, _nuget_loaded

    from pythonnet import _LOADED

    # -------------------------------
    # 1. Initialize runtime (ONCE)
    # -------------------------------
    if not _runtime_initialized and not _LOADED:
        from clr_loader import get_coreclr
        from pythonnet import set_runtime

        runtime_config_path = current_dir / "dotnet_lib" / "dotnet.runtime.config.json"
        rt = get_coreclr(runtime_config=str(runtime_config_path))
        set_runtime(rt)

        sempy.fabric._client._utils._init_analysis_services()
        _runtime_initialized = True

    # -------------------------------
    # 2. Load NuGet assemblies (ONCE)
    # -------------------------------
    if _nuget_loaded:
        return

    def load_pkg(name, version):
        if not is_assembly_loaded(name):
            download_and_load_nuget_package(name, version)

    # load_pkg("System.Text.Json", "7.0.4")
    load_pkg("Newtonsoft.Json", "13.0.3")
    load_pkg("NuGet.Versioning", "6.4.0")
    load_pkg("NuGet.Frameworks", "6.4.0")
    load_pkg("System.IO.Packaging", "7.0.0")

    _nuget_loaded = True

    # DAXLib
    DAX_LIB_VERSION = "0.5.3-beta"
    DAX_LIB_ASSEMBLIES = [
        "DaxLib.Client",
    ]

    for name in DAX_LIB_ASSEMBLIES:
        download_and_load_nuget_package(
            name, DAX_LIB_VERSION, nuget_dir, load_assembly=True
        )

    from System.Reflection import Assembly

    # VPAX
    VPA_VERSION = "1.10.0"
    VPA_ASSEMBLIES = [
        "Dax.Metadata",
        "Dax.Model.Extractor",
        "Dax.ViewVpaExport",
        "Dax.Vpax",
    ]

    for name in VPA_ASSEMBLIES:
        download_and_load_nuget_package(
            name, VPA_VERSION, nuget_dir, load_assembly=False
        )

    # For some reason I have to load these after and not inside the download_and_load_nuget_package function
    dll_paths = [
        f"{nuget_dir}/Dax.Model.Extractor_1.10.0/lib/net6.0/Dax.Model.Extractor.dll",
        f"{nuget_dir}/Dax.Metadata_1.10.0/lib/netstandard2.0/Dax.Metadata.dll",
        f"{nuget_dir}/Dax.ViewVpaExport_1.10.0/lib/netstandard2.0/Dax.ViewVpaExport.dll",
        f"{nuget_dir}/Dax.Vpax_1.10.0/lib/net6.0/Dax.Vpax.dll",
    ]
    for dll_path in dll_paths:
        Assembly.LoadFile(dll_path)

    _runtime_set = True
