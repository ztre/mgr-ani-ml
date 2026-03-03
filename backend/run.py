"""Development runner."""
from __future__ import annotations

import os
import sys
from pathlib import Path

import uvicorn


if __name__ == "__main__":
    project_root = Path(__file__).resolve().parent.parent
    project_root_str = str(project_root)

    # Ensure `backend.main` remains importable in reload subprocesses.
    if project_root_str not in sys.path:
        sys.path.insert(0, project_root_str)

    py_path = os.environ.get("PYTHONPATH", "")
    parts = [p for p in py_path.split(os.pathsep) if p] if py_path else []
    if project_root_str not in parts:
        os.environ["PYTHONPATH"] = (
            project_root_str if not py_path else project_root_str + os.pathsep + py_path
        )

    uvicorn.run(
        "backend.main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        reload_dirs=[str(project_root / "backend")],
    )
