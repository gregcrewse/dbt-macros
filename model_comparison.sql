# model_test.py
import os
import sys
import subprocess
import argparse
from pathlib import Path
import datetime
import json
import csv

def find_model_path(model_name):
    """Find the full path to a model."""
    try:
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

        # If full path is provided
        if model_name.endswith('.sql'):
            path = Path(model_name)
            if path.exists():
                return path
            model_name = path.stem

        # Search for the model file in models directory
        models_dir = project_root / 'models'
        matches = list(models_dir.rglob(f"*{model_name}.sql"))
        
        if not matches:
            print(f"Could not find model {model_name}")
            return None
            
        if len(matches) > 1:
            print(f"Found multiple matches for {model_name}:")
            for match in matches:
                print(f"  {match}")
            print("Please specify the model more precisely")
            return None
            
        return matches[0]

    except Exception as e:
        print(f"Error in find_model_path: {str(e)}")
        return None

def get_main_branch_content(model_path):
    """Get content of the file from main branch."""
    try:
        # Get the git root directory
        git_root = subprocess.run(
            ['git', 'rev-parse', '--show-toplevel'],
            capture_output=True,
            text=True,
            check=True
        ).stdout.strip()
        
        # Convert model_path to be relative to git root
        git_root_path = Path(git_root)
        try:
            relative_path = model_path.relative_to(git_root_path)
        except ValueError:
            relative_path = model_path
        
        print(f"Looking for file in main branch at: {relative_path}")
        
        result = subprocess.run(
            ['git', 'show', f'main:{relative_path}'], 
            capture_output=True, 
            text=True,
            check=True
        )
        return result.stdout
    except subprocess.CalledProcessError as e:
        print(f"Warning: Could not find {relative_path} in main branch")
        print(f"Git error: {e.stderr.decode()}")
        return None
    except Exception as e:
        print(f"Error accessing main branch content: {str(e)}")
        return None

def create_temp_model(content, suffix, original_name, model_dir):
    """Create a temporary copy of the model."""
    try:
        temp_name = f"temp_{original_name}_{suffix}"
        
        # Create a temporary directory for analysis models if it doesn't exist
        analysis_dir = model_dir / 'analysis'
        analysis_dir.mkdir(exist_ok=True)
        
        temp_path = analysis_dir / f"{temp_name}.sql"

        # Extract all ref() calls using regex
        import re
        refs = re.findall(r"ref\(['\"]([^'\"]+)['\"]\)", content)
        print(f"Found refs: {refs}")

        # Copy the content
        modified_content = content

        # Only replace the ref for the model we're comparing
        if original_name in refs:
            modified_content = re.sub(
                f"ref\\(['\"]({original_name})['\"]\\)", 
                f"ref('{temp_name}')", 
                modified_content
            )
        
        # Add config at the top to ensure it's materialized as a table
        config_block = '''{{
    config(
        materialized='table'
    )
}}\n\n'''
        modified_content = config_block + modified_content
        
        with open(temp_path, 'w') as f:
            f.write(modified_content)
        
        return temp_path, temp_name
        
    except Exception as e:
        print(f"Error creating temporary model: {e}")
        return None, None


def create_comparison_macro(model1_name: str, model2_name: str) -> Path:
    """Create a macro file for model comparison."""
    macro_content = '''
{% macro compare_versions() %}
    {% set relation1 = ref(\'''' + model1_name + '''\') %}
    {% set relation2 = ref(\'''' + model2_name + '''\') %}
    
    {% set query %}
        with version1_stats as (
            select count(*) as row_count
            from {{ relation1 }}
        ),
        version2_stats as (
            select count(*) as row_count
            from {{ relation2 }}
        )
        select 
            version1_stats.row_count as main_row_count,
            version2_stats.row_count as current_row_count,
            version2_stats.row_count - version1_stats.row_count as row_difference
        from version1_stats, version2_stats
    {% endset %}

    {% set results = run_query(query) %}
    {% do results.print_table() %}
    
    {% do log(results.columns | string, info=true) %}
    {% do log(results.rows | string, info=true) %}

{% endmacro %}
'''
    
    macros_dir = Path('macros')
    macros_dir.mkdir(exist_ok=True)
    
    macro_path = macros_dir / 'compare_versions.sql'
    with open(macro_path, 'w') as f:
        f.write(macro_content)
    
    return macro_path

def save_results(results_json: str, output_dir: Path, model_name: str) -> Path:
    """Save comparison results to files."""
    timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
    result_dir = output_dir / f'{model_name}_comparison_{timestamp}'
    result_dir.mkdir(parents=True, exist_ok=True)
    
    try:
        # Find the results in the output
        row_data = None
        lines = results_json.splitlines()
        for i, line in enumerate(lines):
            if "[" in line and "]" in line:  # Look for the array output
                try:
                    # Parse the line containing the results array
                    row_data = json.loads(line.strip())
                    break
                except json.JSONDecodeError:
                    continue

        if not row_data:
            print("No comparison data found in output")
            return None

        # Save summary
        with open(result_dir / 'summary.txt', 'w') as f:
            f.write(f"Comparison Results for {model_name}\n")
            f.write("=" * 50 + "\n\n")
            
            f.write("Row Counts:\n")
            f.write("-" * 20 + "\n")
            f.write(f"Main branch rows: {row_data[0]['main_row_count']}\n")
            f.write(f"Current branch rows: {row_data[0]['current_row_count']}\n")
            f.write(f"Difference: {row_data[0]['row_difference']}\n")
        
        print(f"\nResults saved to: {result_dir}/summary.txt")
        return result_dir
        
    except Exception as e:
        print(f"Error saving results: {e}")
        print(f"Raw output was:")
        print(results_json)
        return None

def main():
    parser = argparse.ArgumentParser(description='Compare dbt model versions')
    parser.add_argument('model_name', help='Name of the model to compare')
    parser.add_argument('--output-dir', type=Path, default=Path('model_comparisons'),
                        help='Directory to save comparison results')
    
    args = parser.parse_args()
    
    # Initialize paths as None
    main_path = None
    current_path = None
    macro_path = None
    
    try:
        # Find the model
        model_path = find_model_path(args.model_name)
        if not model_path:
            sys.exit(1)
        
        print(f"Found model at: {model_path}")
        
        # Get original and main branch content
        with open(model_path, 'r') as f:
            current_content = f.read()
        
        main_content = get_main_branch_content(model_path)
        if not main_content:
            sys.exit(1)
        
        # Create temporary models
        model_dir = model_path.parent
        original_name = model_path.stem
        
        main_path, main_name = create_temp_model(
            main_content, 'main', original_name, model_dir)
        current_path, current_name = create_temp_model(
            current_content, 'current', original_name, model_dir)
        
        if not main_path or not current_path:
            print("Failed to create temporary models")
            sys.exit(1)
        
        print(f"Created temporary models: {main_name} and {current_name}")
        
        # Create comparison macro
        macro_path = create_comparison_macro(main_name, current_name)
        if not macro_path:
            print("Failed to create comparison macro")
            sys.exit(1)
        
        # Run models
        print("\nRunning models...")
        try:
            subprocess.run(
                ['dbt', 'run', '--models', f"{main_name} {current_name}", '--target', 'dev', '--quiet'],
                check=True,
                text=True
            )
        except subprocess.CalledProcessError as e:
            print("\nError running models. Check dbt logs for details.")
            sys.exit(1)
        
        # Run comparison
        print("Running comparison...")
        try:
            result = subprocess.run(
                ['dbt', 'run-operation', 'compare_versions', '--target', 'dev'],
                capture_output=True,
                text=True,
                check=True
            )
        except subprocess.CalledProcessError as e:
            print("\nError running comparison.")
            print("\nCommand output:")
            print(e.stdout)
            print("\nError output:")
            print(e.stderr)
            try:
                # Try to get the latest log content
                log_path = Path('logs/dbt.log')
                if log_path.exists():
                    with open(log_path, 'r') as f:
                        # Read last 20 lines of log
                        logs = f.readlines()[-20:]
                        print("\nRelevant log content:")
                        print(''.join(logs))
                else:
                    print("Could not find dbt log file at logs/dbt.log")
            except Exception as log_error:
                print(f"Error reading logs: {log_error}")
            sys.exit(1)        # Run models
        print("\nRunning models...")
        dbt_run_result = subprocess.run(
            ['dbt', 'run', '--models', f"{main_name} {current_name}", '--target', 'dev', '--debug'],
            capture_output=True,
            text=True
        )
        
        if dbt_run_result.returncode != 0:
            print("\nError running models:")
            print("\nStandard output:")
            print(dbt_run_result.stdout)
            print("\nError output:")
            print(dbt_run_result.stderr)
            
            # Print the contents of the temporary files for debugging
            print("\nContents of main branch model file:")
            with open(main_path, 'r') as f:
                print(f.read())
                
            print("\nContents of current branch model file:")
            with open(current_path, 'r') as f:
                print(f.read())
                
            sys.exit(1)
        else:
            print("Model run output:")
            print(dbt_run_result.stdout)
        
    finally:
        # Cleanup
        for path in [main_path, current_path, macro_path]:
            if path and path.exists():
                try:
                    os.remove(path)
                except Exception as e:
                    pass

if __name__ == "__main__":
    main()
