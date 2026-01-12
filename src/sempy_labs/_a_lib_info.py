import sempy
import requests
import zipfile
from pathlib import Path


lib_name = "semanticlinklabs"
lib_version = "0.12.9"

NUGET_BASE_URL = "https://www.nuget.org/api/v2/package"
current_dir = Path(__file__).parent
nuget_dir = current_dir / "nuget_dlls"
_runtime_set = False


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


def set_dotnet_runtime():
    global _runtime_set
    if _runtime_set:
        return

    from clr_loader import get_coreclr
    from pythonnet import set_runtime

    # Load the runtime and set it BEFORE importing clr
    runtime_config_path = current_dir / "dotnet_lib" / "dotnet.runtime.config.json"
    rt = get_coreclr(runtime_config=str(runtime_config_path))
    set_runtime(rt)
    sempy.fabric._client._utils._init_analysis_services()
    _runtime_set = True

    download_and_load_nuget_package("System.Text.Json", "7.0.4")
    download_and_load_nuget_package("Newtonsoft.Json", "13.0.3")
    download_and_load_nuget_package("NuGet.Versioning", "6.4.0")
    download_and_load_nuget_package("NuGet.Frameworks", "6.4.0")
    download_and_load_nuget_package("System.IO.Packaging", "7.0.0")
