import os
import sys
import subprocess
import argparse
from pathlib import Path
import datetime
import json
import csv

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

def create_temp_model(content, suffix, original_name, model_dir):
    """Create a temporary copy of the model."""
    try:
        temp_name = f"temp_{original_name}_{suffix}"
        temp_path = model_dir / f"{temp_name}.sql"
        
        # Replace the model name in any ref() calls
        content = content.replace(f"ref('{original_name}')", f"ref('{temp_name}')")
        
        with open(temp_path, 'w') as f:
            f.write(content)
        
        return temp_path, temp_name
    except Exception as e:
        print(f"Error creating temporary model: {e}")
        return None, None

def create_comparison_macro(model1_name: str, model2_name: str) -> Path:
    """Create a macro file for model comparison."""
    macro_content = f'''
{{% macro compare_versions() %}}
    -- Get column information from both versions
    {{% set cols1 = adapter.get_columns_in_relation(ref('{model1_name}')) %}}
    {{% set cols2 = adapter.get_columns_in_relation(ref('{model2_name}')) %}}
    
    {{% set common_cols = [] %}}
    {{% set version1_only_cols = [] %}}
    {{% set version2_only_cols = [] %}}
    
    -- Find common and unique columns
    {{% for col1 in cols1 %}}
        {{% set col_in_version2 = false %}}
        {{% for col2 in cols2 %}}
            {{% if col1.name|lower == col2.name|lower %}}
                {{% do common_cols.append(col1.name) %}}
                {{% set col_in_version2 = true %}}
            {{% endif %}}
        {{% endfor %}}
        {{% if not col_in_version2 %}}
            {{% do version1_only_cols.append(col1.name) %}}
        {{% endif %}}
    {{% endfor %}}
    
    {{% for col2 in cols2 %}}
        {{% set col_in_version1 = false %}}
        {{% for col1 in cols1 %}}
            {{% if col2.name|lower == col1.name|lower %}}
                {{% set col_in_version1 = true %}}
            {{% endif %}}
        {{% endfor %}}
        {{% if not col_in_version1 %}}
            {{% do version2_only_cols.append(col2.name) %}}
        {{% endif %}}
    {{% endfor %}}
    
    -- Compare data
    WITH version1_stats AS (
        SELECT
            'version1' as version,
            COUNT(*) as row_count
            {{% for col in common_cols %}}
            , COUNT({{ col }}) as {{ col }}_non_null_count
            , COUNT(DISTINCT {{ col }}) as {{ col }}_distinct_count
            {{% endfor %}}
        FROM {{{{ ref('{model1_name}') }}}}
    ),
    version2_stats AS (
        SELECT
            'version2' as version,
            COUNT(*) as row_count
            {{% for col in common_cols %}}
            , COUNT({{ col }}) as {{ col }}_non_null_count
            , COUNT(DISTINCT {{ col }}) as {{ col }}_distinct_count
            {{% endfor %}}
        FROM {{{{ ref('{model2_name}') }}}}
    ),
    column_diffs AS (
        SELECT 'Column differences' as comparison_type,
        '{", ".join(common_cols)}' as common_columns,
        '{", ".join(version1_only_cols)}' as main_branch_only_columns,
        '{", ".join(version2_only_cols)}' as current_branch_only_columns
    ),
    stats_diff AS (
        SELECT
            'Statistics' as comparison_type,
            v1.row_count as main_branch_rows,
            v2.row_count as current_branch_rows,
            v2.row_count - v1.row_count as row_difference
            {{% for col in common_cols %}}
            , v1.{{ col }}_non_null_count as {{ col }}_main_non_nulls
            , v2.{{ col }}_non_null_count as {{ col }}_current_non_nulls
            , v1.{{ col }}_distinct_count as {{ col }}_main_distinct
            , v2.{{ col }}_distinct_count as {{ col }}_current_distinct
            {{% endfor %}}
        FROM version1_stats v1
        CROSS JOIN version2_stats v2
    )
    SELECT * FROM column_diffs
    UNION ALL
    SELECT * FROM stats_diff
{{% endmacro %}}
'''
    
    macros_dir = Path('macros')
    macros_dir.mkdir(exist_ok=True)
    macro_path = macros_dir / 'compare_versions.sql'
    
    with open(macro_path, 'w') as f:
        f.write(macro_content)
    
    return macro_path

def save_results(results_json, output_dir: Path, model_name: str):
    """Save comparison results to CSV files."""
    timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
    result_dir = output_dir / f'{model_name}_comparison_{timestamp}'
    result_dir.mkdir(parents=True, exist_ok=True)
    
    try:
        results = json.loads(results_json)
        
        # Save summary
        with open(result_dir / 'summary.txt', 'w') as f:
            f.write(f"Comparison Results for {model_name}\n")
            f.write("=" * 50 + "\n\n")
            
            f.write("Column Changes:\n")
            f.write("-" * 20 + "\n")
            f.write(f"Common columns: {results['common_columns']}\n")
            f.write(f"Main branch only: {results['main_branch_only_columns']}\n")
            f.write(f"Current branch only: {results['current_branch_only_columns']}\n\n")
            
            f.write("Row Counts:\n")
            f.write("-" * 20 + "\n")
            f.write(f"Main branch rows: {results['main_branch_rows']}\n")
            f.write(f"Current branch rows: {results['current_branch_rows']}\n")
            f.write(f"Difference: {results['row_difference']}\n")
            
            f.write("\nColumn Statistics:\n")
            f.write("-" * 20 + "\n")
            for col in results['column_stats']:
                f.write(f"\n{col}:\n")
                f.write(f"  Non-null counts - Main: {results[f'{col}_main_non_nulls']}, ")
                f.write(f"Current: {results[f'{col}_current_non_nulls']}\n")
                f.write(f"  Distinct values - Main: {results[f'{col}_main_distinct']}, ")
                f.write(f"Current: {results[f'{col}_current_distinct']}\n")
        
        print(f"\nResults saved to: {result_dir}")
        return result_dir
        
    except Exception as e:
        print(f"Error saving results: {e}")
        return None

def main():
    parser = argparse.ArgumentParser(description='Compare dbt model versions')
    parser.add_argument('model_path', help='Path to the model to compare')
    parser.add_argument('--output-dir', type=Path, default=Path('model_comparisons'),
                        help='Directory to save comparison results')
    
    args = parser.parse_args()
    model_path = Path(args.model_path)
    
    try:
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
        
        print(f"Created temporary models: {main_name} and {current_name}")
        
        # Create comparison macro
        macro_path = create_comparison_macro(main_name, current_name)
        print("Created comparison macro")
        
        # Run models and comparison
        print("\nRunning models...")
        subprocess.run(['dbt', 'run', '--models', f"{main_name} {current_name}"], check=True)
        
        print("\nComparing versions...")
        result = subprocess.run(
            ['dbt', 'run-operation', 'compare_versions', '--log-format', 'json'],
            capture_output=True,
            text=True,
            check=True
        )
        
        # Save results
        result_dir = save_results(result.stdout, args.output_dir, original_name)
        
    finally:
        # Cleanup
        for path in [main_path, current_path, macro_path]:
            if path and path.exists():
                try:
                    os.remove(path)
                except Exception as e:
                    print(f"Warning: Could not remove temporary file {path}: {e}")

if __name__ == "__main__":
    main()
