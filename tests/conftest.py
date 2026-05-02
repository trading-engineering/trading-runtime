from __future__ import annotations

import sys
from pathlib import Path


def _ensure_workspace_import_paths() -> None:
    workspace_root = Path(__file__).resolve().parents[2]
    runtime_root = workspace_root / "core-runtime"
    core_root = workspace_root / "core"

    for path in (runtime_root, core_root):
        path_str = str(path)
        if path_str not in sys.path:
            sys.path.insert(0, path_str)


_ensure_workspace_import_paths()
