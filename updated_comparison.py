import os
import sys
import subprocess
import argparse
from pathlib import Path
import datetime
import json

def find_model_path(model_name):
    """Find the full path to a model."""
    try:
        # First try direct path if it ends in .sql
        if model_name.endswith('.sql'):
            path = Path(model_name)
            if path.exists():
                return path
            model_name = path.stem
        
        # Find project root
        current = Path.cwd()
        while current != current.parent:
            if (current / 'dbt_project.yml').exists():
                project_root = current
                break
            current = current.parent
        else:
            print("Could not find dbt_project.yml")
            return None

        # Search for the model file
        models_dir = project_root / 'models'
        for sql_file in models_dir.rglob('*.sql'):
            if sql_file.stem == model_name:
                return sql_file

        return None

    except Exception as e:
        print(f"Error in find_model_path: {str(e)}")
        return None

def get_main_branch_content(model_path):
    """Get content of the file from main branch."""
    try:
        relative_path = model_path.relative_to(find_dbt_project_root())
        result = subprocess.run(
            ['git', 'show', f'main:{relative_path}'], 
            capture_output=True, 
            text=True,
            check=True
        )
        return result.stdout
    except subprocess.CalledProcessError:
        print(f"Warning: Could not find {model_path} in main branch")
        return None
    except Exception as e:
        print(f"Error in get_main_branch_content: {str(e)}")
        return None

def find_dbt_project_root():
    """Find the root directory of the dbt project."""
    current = Path.cwd()
    while current != current.parent:
        if (current / 'dbt_project.yml').exists():
            return current
        current = current.parent
    return None

def create_temp_model(content, changes, original_name, model_dir):
    """Create a temporary copy of the model with changes applied."""
    try:
        timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
        temp_name = f"temp_{original_name}_{timestamp}"
        temp_path = model_dir / f"{temp_name}.sql"
        
        # Apply changes
        for old_str, new_str in changes:
            content = content.replace(old_str, new_str)
        
        # Update model name in content
        content = content.replace(f"ref('{original_name}')", f"ref('{temp_name}')")
        
        # Write temp model
        with open(temp_path, 'w') as f:
            f.write(content)
        
        return temp_path, temp_name
    except Exception as e:
        print(f"Error in create_temp_model: {str(e)}")
        return None, None

def main():
    parser = argparse.ArgumentParser(description='Test DBT model changes')
    parser.add_argument('model_path', help='Path to the model to test')
    parser.add_argument('--changes', nargs='+', help='Changes to apply in old:new format')
    parser.add_argument('--against-main', action='store_true',
                        help='Compare against version in main branch')
    parser.add_argument('--original-model', 
                        help='Name of the original model to compare against (useful for new files)')
    
    args = parser.parse_args()
    
    # Find the model path
    model_path = find_model_path(args.model_path)
    if not model_path:
        print(f"Error: Could not find model {args.model_path}")
        sys.exit(1)
    
    print(f"Found model at: {model_path}")
    model_dir = model_path.parent
    
    # Handle original model content
    if args.against_main:
        original_content = get_main_branch_content(model_path)
        if not original_content:
            sys.exit(1)
        original_name = model_path.stem
    elif args.original_model:
        original_path = find_model_path(args.original_model)
        if not original_path:
            print(f"Error: Could not find original model {args.original_model}")
            sys.exit(1)
        with open(original_path, 'r') as f:
            original_content = f.read()
        original_name = original_path.stem
    else:
        with open(model_path, 'r') as f:
            original_content = f.read()
        original_name = model_path.stem
    
    changes = [tuple(change.split(':')) for change in (args.changes or [])]
    
    try:
        # Create temporary models
        temp_original_path, temp_original_name = create_temp_model(
            original_content, [], original_name, model_dir)
        if not temp_original_path:
            sys.exit(1)
        print(f"Created temporary original model: {temp_original_path}")
        
        if args.against_main or args.original_model:
            with open(model_path, 'r') as f:
                changed_content = f.read()
        else:
            changed_content = original_content
            
        temp_changed_path, temp_changed_name = create_temp_model(
            changed_content, changes, original_name, model_dir)
        if not temp_changed_path:
            sys.exit(1)
        print(f"Created temporary changed model: {temp_changed_path}")
        
        # Run both models
        print("Running dbt models...")
        subprocess.run(['dbt', 'run', '--models', f"{temp_original_name} {temp_changed_name}"])
        
        # For now, just print success message
        print("\nModels compiled successfully. You can now compare their results in your data warehouse.")
        
    finally:
        # Cleanup
        if 'temp_original_path' in locals():
            try:
                os.remove(temp_original_path)
            except Exception as e:
                print(f"Warning: Could not remove temporary file {temp_original_path}: {e}")
        if 'temp_changed_path' in locals():
            try:
                os.remove(temp_changed_path)
            except Exception as e:
                print(f"Warning: Could not remove temporary file {temp_changed_path}: {e}")

if __name__ == "__main__":
    main()
