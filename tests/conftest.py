"""Pytest configuration for the test suite.

The library depends on the optional ``semantic-link-sempy`` package, which is
generally only available inside Microsoft Fabric notebooks. To make our unit
tests runnable in any environment (CI, local dev, etc.), we install lightweight
stand-in modules for ``sempy`` and ``sempy.fabric`` *before* any test module
imports ``sempy_labs``. The stubs only need to satisfy the import paths the
code under test traverses; production behaviour is exercised through the real
package in notebooks.
"""

from __future__ import annotations

import os
import sys
import types


# Make ``src/sempy_labs`` importable without installing the package.
_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_SRC_PATH = os.path.join(_REPO_ROOT, "src")
if _SRC_PATH not in sys.path:
    sys.path.insert(0, _SRC_PATH)


def _ensure_stub(module_name: str) -> types.ModuleType:
    """Return ``sys.modules[module_name]`` creating a stub module if needed."""
    if module_name in sys.modules:
        return sys.modules[module_name]
    mod = types.ModuleType(module_name)
    sys.modules[module_name] = mod
    return mod


try:  # Use the real package if it is installed.
    import sempy  # noqa: F401
    import sempy._utils._log  # noqa: F401
except ImportError:
    sempy_mod = _ensure_stub("sempy")
    sempy_utils = _ensure_stub("sempy._utils")
    sempy_log = _ensure_stub("sempy._utils._log")

    def _log_decorator(func=None, **_kwargs):
        """No-op stand-in for ``sempy._utils._log.log``.

        Supports both ``@log`` (bare) and ``@log(...)`` (with kwargs) usage.
        """
        if func is None:
            return lambda f: f
        return func

    sempy_log.log = _log_decorator
    sempy_mod._utils = sempy_utils
    sempy_utils._log = sempy_log

# ``sempy.fabric`` is imported lazily inside functions in production code, but
# the stub keeps tooling like ``importlib`` happy if any test reaches for it.
if "sempy.fabric" not in sys.modules:
    _ensure_stub("sempy.fabric")
