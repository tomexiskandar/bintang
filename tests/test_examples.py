
import os
import subprocess
import sys
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]
EXAMPLES_DIR = ROOT / "examples"
SRC_DIR = ROOT / "src"


def get_example_scripts():
    """Discover example scripts and mark local-only tests."""
    scripts = []

    for script in sorted(EXAMPLES_DIR.glob("*.py")):
        if script.name.startswith("local_"):
            scripts.append(
                pytest.param(
                    script,
                    marks=pytest.mark.local,
                    id=script.name
                )
            )
        else:
            scripts.append(pytest.param(script, id=script.name))

    return scripts


@pytest.mark.parametrize("script_path", get_example_scripts())
def test_example_execution(script_path):
    """Run each example script and verify successful execution."""

    env = os.environ.copy()

    # Make local bintang source code importable
    env["PYTHONPATH"] = (
        str(SRC_DIR)
        + os.pathsep
        + env.get("PYTHONPATH", "")
    )

    result = subprocess.run(
        [sys.executable, str(script_path)],
        cwd=ROOT,
        capture_output=True,
        text=True,
        env=env
    )

    assert result.returncode == 0, (
        f"Example {script_path.name} failed!\n"
        f"STDOUT:\n{result.stdout}\n"
        f"STDERR:\n{result.stderr}"
    )
