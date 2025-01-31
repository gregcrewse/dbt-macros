import os
import sys
import subprocess
import argparse
from pathlib import Path
import datetime
import pandas as pd
import sqlalchemy
from typing import Tuple
import psycopg2

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

def create_temp_model(content, changes, original_name, model_dir) -> Tuple[Path, str]:
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

def get_connection():
    """Get Redshift connection from dbt profile."""
    try:
        # Get profile info using dbt debug
        debug_result = subprocess.run(
            ['dbt', 'debug', '--target', 'prod'],  # or whatever your target is
            capture_output=True,
            text=True
        )
        
        # Parse connection info from profiles.yml
        home = str(Path.home())
        profile_path = Path(home) / '.dbt' / 'profiles.yml'
        
        if not profile_path.exists():
            raise Exception(f"Could not find dbt profiles at {profile_path}")
            
        import yaml
        with open(profile_path) as f:
            profiles = yaml.safe_load(f)
            
        # Get the active profile and target
        with open('dbt_project.yml') as f:
            project = yaml.safe_load(f)
            profile_name = project['profile']
        
        # Create SQLAlchemy engine for Redshift
        profile = profiles[profile_name]['outputs']['prod']  # or whatever your target is
        conn_string = f"postgresql://{profile['user']}:{profile['pass']}@{profile['host']}:{profile['port']}/{profile['dbname']}"
        
        return sqlalchemy.create_engine(conn_string)
            
    except Exception as e:
        print(f"Error getting database connection: {e}")
        sys.exit(1)

def compare_models(engine, original_name: str, changed_name: str) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Compare two models and return comparison DataFrames."""
    try:
        # Get column information from both models
        original_cols = pd.read_sql(f"SELECT * FROM {original_name} LIMIT 0", engine).columns
        changed_cols = pd.read_sql(f"SELECT * FROM {changed_name} LIMIT 0", engine).columns
        
        # Compare row counts
        row_counts = pd.DataFrame({
            'Model': ['Original', 'Changed'],
            'Count': [
                pd.read_sql(f"SELECT COUNT(*) FROM {original_name}", engine).iloc[0, 0],
                pd.read_sql(f"SELECT COUNT(*) FROM {changed_name}", engine).iloc[0, 0]
            ]
        })
        
        # Compare columns
        all_cols = list(set(original_cols) | set(changed_cols))
        column_changes = pd.DataFrame({
            'Column': all_cols,
            'In_Original': [col in original_cols for col in all_cols],
            'In_Changed': [col in changed_cols for col in all_cols]
        })
        
        # Sample differences (if models have common columns)
        common_cols = list(set(original_cols) & set(changed_cols))
        if common_cols:
            diffs = pd.read_sql(f"""
                SELECT DISTINCT *
                FROM (
                    SELECT {', '.join(common_cols)} FROM {original_name}
                    EXCEPT
                    SELECT {', '.join(common_cols)} FROM {changed_name}
                ) diff
                LIMIT 5
            """, engine)
        else:
            diffs = pd.DataFrame()
        
        return row_counts, column_changes, diffs
        
    except Exception as e:
        print(f"Error comparing models: {e}")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

def save_comparison_results(output_dir: Path, original_name: str, changed_name: str,
                          row_counts: pd.DataFrame, column_changes: pd.DataFrame, diffs: pd.DataFrame):
    """Save comparison results to CSV files."""
    timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
    result_dir = output_dir / f'comparison_{timestamp}'
    result_dir.mkdir(parents=True, exist_ok=True)
    
    # Save summary
    with open(result_dir / 'summary.txt', 'w') as f:
        f.write(f"Comparison between {original_name} and {changed_name}\n\n")
        f.write("Row Counts:\n")
        f.write(row_counts.to_string())
        f.write("\n\nColumn Changes:\n")
        f.write(column_changes.to_string())
    
    # Save detailed results
    if not column_changes.empty:
        column_changes.to_csv(result_dir / 'column_changes.csv', index=False)
    if not diffs.empty:
        diffs.to_csv(result_dir / 'value_differences.csv', index=False)
    
    return result_dir

def main():
    parser = argparse.ArgumentParser(description='Test DBT model changes')
    parser.add_argument('model_path', help='Path to the model to test')
    parser.add_argument('--changes', nargs='+', help='Changes to apply in old:new format')
    parser.add_argument('--against-main', action='store_true',
                        help='Compare against version in main branch')
    parser.add_argument('--original-model', 
                        help='Name of the original model to compare against (useful for new files)')
    parser.add_argument('--output-dir', type=Path, default=Path('model_comparisons'),
                        help='Directory to save comparison results')
    
    args = parser.parse_args()
    
    # Initialize temp model paths and names
    temp_original_path = None
    temp_changed_path = None
    temp_original_name = None
    temp_changed_name = None
    
    try:
        # Find the model path
        model_path = find_model_path(args.model_path)
        if not model_path:
            print(f"Error: Could not find model {args.model_path}")
            sys.exit(1)
        
        print(f"Found model at: {model_path}")
        model_dir = model_path.parent
        
        # Get original content
        if args.against_main:
            original_content = get_main_branch_content(model_path)
            if not original_content:
                sys.exit(1)
            original_name = model_path.stem
        else:
            with open(model_path, 'r') as f:
                original_content = f.read()
            original_name = model_path.stem
        
        # Create temporary models
        temp_original_path, temp_original_name = create_temp_model(
            original_content, [], original_name, model_dir)
        print(f"Created temporary original model: {temp_original_path}")
        
        # Get changed content
        with open(model_path, 'r') as f:
            changed_content = f.read()
        
        # Apply changes if any
        changes = [tuple(change.split(':')) for change in (args.changes or [])]
        temp_changed_path, temp_changed_name = create_temp_model(
            changed_content, changes, original_name, model_dir)
        print(f"Created temporary changed model: {temp_changed_path}")
        
        # Run both models
        print("\nRunning dbt models...")
        subprocess.run(['dbt', 'run', '--models', f"{temp_original_name} {temp_changed_name}"], check=True)
        
        # Get connection and compare
        print("\nComparing models...")
        engine = get_connection()
        row_counts, column_changes, diffs = compare_models(engine, temp_original_name, temp_changed_name)
        
        # Save results
        print("\nSaving results...")
        result_dir = save_comparison_results(
            args.output_dir, temp_original_name, temp_changed_name,
            row_counts, column_changes, diffs
        )
        print(f"Results saved in: {result_dir}")
        
    finally:
        # Cleanup
        if temp_original_path and temp_original_path.exists():
            try:
                os.remove(temp_original_path)
            except Exception as e:
                print(f"Warning: Could not remove {temp_original_path}: {e}")
        
        if temp_changed_path and temp_changed_path.exists():
            try:
                os.remove(temp_changed_path)
            except Exception as e:
                print(f"Warning: Could not remove {temp_changed_path}: {e}")

if __name__ == "__main__":
    main()
