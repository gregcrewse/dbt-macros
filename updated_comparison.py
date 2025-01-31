import subprocess
import json
from pathlib import Path

def main():
    print("\nBasic DBT Environment Check:")
    
    # 1. Check if dbt is installed and accessible
    print("\n1. Checking dbt installation...")
    try:
        version_result = subprocess.run(['dbt', '--version'], capture_output=True, text=True)
        print(f"DBT Version output: {version_result.stdout}")
    except FileNotFoundError:
        print("ERROR: dbt command not found. Is dbt installed and in your PATH?")
        return
    except Exception as e:
        print(f"Error running dbt --version: {e}")
        return

    # 2. Print current working directory
    print(f"\n2. Current working directory: {Path.cwd()}")

    # 3. Look for dbt_project.yml
    print("\n3. Looking for dbt_project.yml...")
    current = Path.cwd()
    found_project = False
    while current != current.parent:
        if (current / 'dbt_project.yml').exists():
            print(f"Found dbt_project.yml at: {current}")
            found_project = True
            
            # List contents of models directory
            models_dir = current / 'models'
            if models_dir.exists():
                print(f"\nContents of {models_dir}:")
                for item in models_dir.rglob('*.sql'):
                    print(f"  {item.relative_to(models_dir)}")
            break
        current = current.parent
    
    if not found_project:
        print("Could not find dbt_project.yml in any parent directory")
        return

    # 4. Try dbt list
    print("\n4. Running dbt list...")
    try:
        list_result = subprocess.run(
            ['dbt', 'list', '--resource-type', 'model'],
            capture_output=True,
            text=True
        )
        print("DBT List output:")
        print(list_result.stdout)
        if list_result.stderr:
            print("DBT List errors:")
            print(list_result.stderr)
    except Exception as e:
        print(f"Error running dbt list: {e}")

if __name__ == "__main__":
    main()
