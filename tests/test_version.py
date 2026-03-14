import bintang
from importlib.metadata import version as get_metadata_version
from packaging.version import Version  # Change this line

def test_version_not_exceeding_limit():
    """Fail the test if the version is 1.2.0 or higher"""
    # Get the current version string
    current_v_str = get_metadata_version("bintang")
    limit_v_str = "1.2.0"
    
    # Use the Version class to compare
    assert Version(current_v_str) < Version(limit_v_str), \
        f"Version {current_v_str} has exceeded the limit of {limit_v_str}!"