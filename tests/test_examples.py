import subprocess
import glob
import pytest
import os
import sys


example_scripts = glob.glob(os.path.join("examples", "*.py"))

@pytest.mark.parametrize(
        "script_path", 
        [
        pytest.param(
            script,
            marks=pytest.mark.local
        ) if os.path.basename(script).startswith("local_")
        else script
        for script in example_scripts
    ]
)
def test_example_execution(script_path):
    """
    to test each file under example folder
    """
    print(script_path)
    # Get the current system environment variables
    env = os.environ.copy()
    
    # Add the 'src' folder to the PYTHONPATH so the script can find 'bintang'
    # This works regardless of whether you are on Windows or Mac/Linux
    src_path = os.path.abspath("src")
    env["PYTHONPATH"] = src_path + os.pathsep + env.get("PYTHONPATH", "")

    result = subprocess.run(
        [sys.executable, script_path], # sys.executable ensures it uses the same python as pytest
        capture_output=True, 
        text=True,
        env=env  # Pass the updated environment here
    )
    
    assert result.returncode == 0, f"Example {script_path} failed!\nError: {result.stderr}"