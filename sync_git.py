import subprocess
import sys

def sync_git():
    if len(sys.argv) < 2:
        print("Error: Missing commit message.")
        print("Usage: python git_sync.py 'message'")
        return

    commit_message = sys.argv[1]

    try:
        print("Adding files...")
        subprocess.run(["git", "add", "."], check=True)
        
        print(f"Committing with message: {commit_message}")
        subprocess.run(["git", "commit", "-m", commit_message], check=True)
        
        print("Pushing to origin main...")
        subprocess.run(["git", "push", "origin", "main"], check=True)
        
        print("Success! ✨")
    except subprocess.CalledProcessError as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    sync_git()