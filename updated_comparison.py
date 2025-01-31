import os
import sys
import subprocess
import argparse
from pathlib import Path
import datetime
import pandas as pd
import tempfile
from typing import Tuple, List, Dict
import sqlalchemy

def get_connection():
    """Get database connection from dbt profiles. Returns sqlalchemy engine."""
    try:
        # Get profile info from dbt
        result = subprocess.run(
            ['dbt', 'debug', '--config-dir'],
            capture_output=True,
            text=True
        )
        # Parse the profile info and create connection
        # This is a placeholder - you'll need to implement based on your specific database
        return sqlalchemy.create_engine('your_connection_string')
    except Exception as e:
        print(f"Error getting database connection: {e}")
        sys.exit(1)

def compare_models(engine, original_model: str, changed_model: str) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Compare two models and return DataFrames with the differences.
    """
    # Row count comparison
    row_counts = pd.read_sql(f"""
        SELECT 
            (SELECT COUNT(*) FROM {original_model}) as original_count,
            (SELECT COUNT(*) FROM {changed_model}) as new_count,
            (SELECT COUNT(*) FROM {changed_model}) - 
            (SELECT COUNT(*) FROM {original_model}) as difference
    """, engine)

    # Column comparison
    column_changes = pd.read_sql(f"""
        WITH original_columns AS (
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = '{original_model}'
        ),
        new_columns AS (
            SELECT column_name, data_type
            FROM information_schema.columns 
            WHERE table_name = '{changed_model}'
        )
        SELECT 
            CASE 
                WHEN o.column_name IS NULL THEN 'Added in new'
                WHEN n.column_name IS NULL THEN 'Removed in new'
                WHEN o.data_type != n.data_type THEN 'Type changed'
                ELSE 'No change'
            END as change_type,
            COALESCE(o.column_name, n.column_name) as column_name,
            o.data_type as original_type,
            n.data_type as new_type
        FROM original_columns o
        FULL OUTER JOIN new_columns n ON o.column_name = n.column_name
        WHERE o.column_name IS NULL 
           OR n.column_name IS NULL 
           OR o.data_type != n.data_type
    """, engine)

    # Sample differences
    diffs = pd.read_sql(f"""
        SELECT 
            o.*, 
            n.*
        FROM {original_model} o
        FULL OUTER JOIN {changed_model} n USING (case_id)
        WHERE (o.case_id IS NULL OR n.case_id IS NULL OR EXISTS (
            SELECT o.*, n.*
            EXCEPT
            SELECT n.*, n.*
        ))
        LIMIT 5
    """, engine)

    return row_counts, column_changes, diffs

def save_comparison_results(
    output_dir: Path,
    row_counts: pd.DataFrame,
    column_changes: pd.DataFrame,
    diffs: pd.DataFrame,
    original_name: str,
    changed_name: str):
    """Save comparison results to CSV files."""
    timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
    output_dir = output_dir / f'comparison_{timestamp}'
    output_dir.mkdir(parents=True, exist_ok=True)

    # Save row count comparison
    with open(output_dir / 'row_count_comparison.txt', 'w') as f:
        f.write(f"Comparison between {original_name} and {changed_name}\n")
        f.write(f"Original count: {row_counts['original_count'].iloc[0]}\n")
        f.write(f"New count: {row_counts['new_count'].iloc[0]}\n")
        f.write(f"Difference: {row_counts['difference'].iloc[0]}\n")

    # Save column changes
    if not column_changes.empty:
        column_changes.to_csv(output_dir / 'column_changes.csv', index=False)
        print(f"\nColumn changes saved to: {output_dir / 'column_changes.csv'}")
    else:
        print("\nNo column changes found")

    # Save row differences
    if not diffs.empty:
        diffs.to_csv(output_dir / 'row_differences.csv', index=False)
        print(f"Row differences saved to: {output_dir / 'row_differences.csv'}")
    else:
        print("No row differences found")

    return output_dir


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
    parser.add_argument('--output-dir', type=Path, default=Path('model_comparisons'),
                        help='Directory to save comparison results')
    
    args = parser.parse_args()
    
    try:
        # [Previous model creation code remains the same...]
        
        # Run both models
        print("Running dbt models...")
        subprocess.run(['dbt', 'run', '--models', f"{temp_original_name} {temp_changed_name}"])
        
        # Get database connection
        engine = get_connection()
        
        # Compare models
        print("\nComparing models...")
        row_counts, column_changes, diffs = compare_models(
            engine, 
            temp_original_name, 
            temp_changed_name
        )
        
        # Save results
        output_dir = save_comparison_results(
            args.output_dir,
            row_counts,
            column_changes,
            diffs,
            temp_original_name,
            temp_changed_name
        )
        
        print(f"\nComparison complete! Results saved in: {output_dir}")
        
    finally:
        # Cleanup
        if 'temp_original_path' in locals():
            try:
                os.remove(temp_original_path)
            except Exception as e:
                print(f"Note: Temporary file {temp_original_path} will be cleaned up later")
        if 'temp_changed_path' in locals():
            try:
                os.remove(temp_changed_path)
            except Exception as e:
                print(f"Note: Temporary file {temp_changed_path} will be cleaned up later")

if __name__ == "__main__":
    main()
