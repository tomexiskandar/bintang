
import argparse
import re
import shutil
import subprocess
import sys
import tomllib
from pathlib import Path


ROOT = Path(__file__).resolve().parent
PYPROJECT = ROOT / "pyproject.toml"

# Change this if your logging module has a different path.
LOG_FILE = ROOT / "src" / "bintang" / "log.py"


def run(*args):
    """Run a command from the repository root and stop on failure."""
    print(f"\n>>> {' '.join(map(str, args))}", flush=True)
    subprocess.run(args, cwd=ROOT, check=True)


def update_log_level():
    """Set the module's log level to ERROR."""
    content = LOG_FILE.read_text(encoding="utf-8")

    pattern = r"(?m)^([ \t]*)log\.setLevel\s*\(.*\)[ \t]*$"

    updated, count = re.subn(
        pattern,
        r"\1log.setLevel(logging.ERROR)",
        content,
    )

    if count != 1:
        raise RuntimeError(
            f"Expected one log.setLevel line in {LOG_FILE}, "
            f"found {count}"
        )

    if "import logging" not in updated:
        raise RuntimeError("Logging module must import logging")

    LOG_FILE.write_text(updated, encoding="utf-8")
    print("Log level set to ERROR")


def increment_version():
    """Increment the patch component of the project version."""
    content = PYPROJECT.read_text(encoding="utf-8")
    project = tomllib.loads(content)["project"]

    old_version = project["version"]
    parts = old_version.split(".")

    if len(parts) != 3 or not all(p.isdigit() for p in parts):
        raise ValueError(
            f"Expected MAJOR.MINOR.PATCH version: {old_version}"
        )

    major, minor, patch = map(int, parts)
    new_version = f"{major}.{minor}.{patch + 1}"

    pattern = (
        r'(?m)^(\s*version\s*=\s*["\'])'
        + re.escape(old_version)
        + r'(["\']\s*)$'
    )

    updated, count = re.subn(
        pattern,
        lambda m: m.group(1) + new_version + m.group(2),
        content,
    )

    if count != 1:
        raise RuntimeError(
            "Could not uniquely update version in pyproject.toml"
        )

    PYPROJECT.write_text(updated, encoding="utf-8")

    print(f"Version: {old_version} -> {new_version}")
    return new_version


def deploy(msg):
    if not msg.strip():
        raise ValueError("Commit message cannot be empty")

    # Save original pyproject.toml content
    original_pyproject = PYPROJECT.read_bytes()

    # save original log.py content
    original_log = LOG_FILE.read_bytes()
    try:
        # Ensure we are deploying from main.
        branch = subprocess.check_output(
            ["git", "branch", "--show-current"],
            cwd=ROOT,
            text=True,
        ).strip()

        if branch != "main":
            raise RuntimeError(
                f"Current branch is '{branch}', expected 'main'"
            )

        # 1. Update logging
        update_log_level()


        # 2. Increase version
        version = increment_version()
        tag = f"v{version}"

        # Prevent accidentally reusing an existing tag.
        existing_tag = subprocess.run(
            ["git", "rev-parse", "-q", "--verify", f"refs/tags/{tag}"],
            cwd=ROOT,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        if existing_tag.returncode == 0:
            raise RuntimeError(f"Tag already exists: {tag}")

        # 3. Remove old distributions
        dist = ROOT / "dist"
        if dist.exists():
            shutil.rmtree(dist)

        # 4. Build package
        run(sys.executable, "-m", "build", "--no-isolation")

        # 5. Install package with optional dependencies
        run(sys.executable, "-m", "pip", "install", "-e", ".[all]")

        # 6. Run pytest
        run(sys.executable, "-m", "pytest", "-v")

        # 7. Sync Git using your existing script
        run(sys.executable, "sync_git.py", msg)

        # 8. Create Git tag
        run("git", "tag", tag)

        # 9. Push Git tag
        run("git", "push", "origin", tag)

        print(f"\nDeployment completed: {tag}")
        print("GitHub updated successfully.")
        print("PyPI upload has NOT been performed.")
    except Exception as exc:
        print(f"\nDEPLOYMENT FAILED: {exc}", file=sys.stderr)
        # Restore original pyproject.toml content
        PYPROJECT.write_bytes(original_pyproject)
        # Restore original log.py content
        LOG_FILE.write_bytes(original_log)
        raise


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Build, test and release bintang"
    )
    parser.add_argument(
        "msg",
        help="Git commit message"
    )

    args = parser.parse_args()

    try:
        deploy(args.msg)
    except (Exception, subprocess.CalledProcessError) as exc:
        print(f"\nDEPLOYMENT FAILED: {exc}", file=sys.stderr)
        sys.exit(1)
