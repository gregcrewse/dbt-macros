import os
import sys
import subprocess
import json
from pathlib import Path

def debug_find_model(model_name):
    """Debug helper to find a dbt model."""
    print("\nDebugging model path search:")
    
    # 1. Check current directory
    print(f"\n1. Current working directory: {Path.cwd()}")
    
    # 2. Try dbt list
    print("\n2. Trying dbt list...")
    try:
        result = subprocess.run(
            ['dbt', 'list', '--resource-type', 'model', '--output', 'json'],
            capture_output=True,
            text=True,
            check=True
        )
        models = json.loads(result.stdout)
        print(f"Found {len(models)} models in dbt list")
        
        for model in models:
            if model.get('name') == model_name:
                path = Path(model.get('original_file_path', ''))
                print(f"Found model in dbt list: {path}")
                if path.exists():
                    print(f"Path exists!")
                    return path
                else:
                    print(f"Path does not exist")
                
    except subprocess.CalledProcessError as e:
        print(f"Error running dbt list: {e}")
        print(f"stderr: {e.stderr}")
    except json.JSONDecodeError as e:
        print(f"Error parsing dbt list output: {e}")
        print(f"Output was: {result.stdout[:200]}...")
    except Exception as e:
        print(f"Unexpected error: {e}")
    
    # 3. Try finding project root
    print("\n3. Looking for dbt_project.yml...")
    current = Path.cwd()
    project_root = None
    while current != current.parent:
        if (current / 'dbt_project.yml').exists():
            project_root = current
            print(f"Found project root: {project_root}")
            break
        current = current.parent
    
    if not project_root:
        print("Could not find dbt_project.yml")
        return None
        
    # 4. Search common directories
    print("\n4. Searching common model directories...")
    common_locations = [
        project_root / 'models',
        project_root / 'models/marts',
        project_root / 'models/intermediate',
        project_root / 'models/staging'
    ]
    
    for location in common_locations:
        print(f"\nChecking {location}")
        if location.exists():
            print(f"Directory exists")
            for file_path in location.rglob(f"{model_name}.sql"):
                print(f"Found file: {file_path}")
                return file_path
        else:
            print(f"Directory does not exist")
    
    print("\nCould not find model path")
    return None

def main():
    if len(sys.argv) != 2:
        print("Usage: python script.py model_name")
        sys.exit(1)
        
    model_name = sys.argv[1]
    model_path = debug_find_model(model_name)
    
    if model_path:
        print(f"\nSuccessfully found model at: {model_path}")
    else:
        print("\nFailed to find model")

if __name__ == "__main__":
    main()
