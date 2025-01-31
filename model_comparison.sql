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
    """
    Creates a dbt macro file that will compare the two versions of the model.
    This macro will be temporarily created in your macros directory and then deleted after use.
    """
    macro_content = '''
{% macro compare_versions() %}
    {%- set relation1 = ref(\'''' + model1_name + '''\') -%}
    {%- set relation2 = ref(\'''' + model2_name + '''\') -%}
    
    {%- set cols1 = adapter.get_columns_in_relation(relation1) -%}
    {%- set cols2 = adapter.get_columns_in_relation(relation2) -%}
    
    {%- set common_cols = [] -%}
    {%- set version1_only_cols = [] -%}
    {%- set version2_only_cols = [] -%}
    
    {%- for col1 in cols1 -%}
        {%- set col_in_version2 = false -%}
        {%- for col2 in cols2 -%}
            {%- if col1.name|lower == col2.name|lower -%}
                {%- do common_cols.append(col1.name) -%}
                {%- set col_in_version2 = true -%}
            {%- endif -%}
        {%- endfor -%}
        {%- if not col_in_version2 -%}
            {%- do version1_only_cols.append(col1.name) -%}
        {%- endif -%}
    {%- endfor -%}
    
    {%- for col2 in cols2 -%}
        {%- set col_in_version1 = false -%}
        {%- for col1 in cols1 -%}
            {%- if col2.name|lower == col1.name|lower -%}
                {%- set col_in_version1 = true -%}
            {%- endif -%}
        {%- endfor -%}
        {%- if not col_in_version1 -%}
            {%- do version2_only_cols.append(col2.name) -%}
        {%- endif -%}
    {%- endfor -%}

    {%- set column_list = common_cols -%}
    
    with base_comparison as (
        select
            '{{ common_cols|join(",") }}' as common_columns,
            '{{ version1_only_cols|join(",") }}' as main_only_columns,
            '{{ version2_only_cols|join(",") }}' as current_only_columns
    ),
    version1_stats as (
        select count(*) as row_count
        {%- for col in column_list %}
        , count({{ col }}) as {{ col }}_non_null_count
        , count(distinct {{ col }}) as {{ col }}_distinct_count
        {%- endfor %}
        from {{ relation1 }}
    ),
    version2_stats as (
        select count(*) as row_count
        {%- for col in column_list %}
        , count({{ col }}) as {{ col }}_non_null_count
        , count(distinct {{ col }}) as {{ col }}_distinct_count
        {%- endfor %}
        from {{ relation2 }}
    ),
    stats_comparison as (
        select
            v1.row_count as main_rows,
            v2.row_count as current_rows,
            v2.row_count - v1.row_count as row_difference
            {%- for col in column_list %}
            , v1.{{ col }}_non_null_count as {{ col }}_main_non_nulls
            , v2.{{ col }}_non_null_count as {{ col }}_current_non_nulls
            , v1.{{ col }}_distinct_count as {{ col }}_main_distinct
            , v2.{{ col }}_distinct_count as {{ col }}_current_distinct
            {%- endfor %}
        from version1_stats v1
        cross join version2_stats v2
    )
    
    select 
        'Results' as result_type,
        base_comparison.*,
        to_json(stats_comparison.*) as statistics
    from base_comparison
    cross join stats_comparison

{% endmacro %}
'''
    
    # Create macros directory if it doesn't exist
    macros_dir = Path('macros')
    macros_dir.mkdir(exist_ok=True)
    
    # Create the macro file
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
        # Parse the results
        for line in results_json.splitlines():
            if '"result_type": "Results"' in line:
                results = json.loads(line)
                column_changes = json.loads(results['column_changes'])
                statistics = json.loads(results['statistics'])
                break
        
        # Save summary
        with open(result_dir / 'summary.txt', 'w') as f:
            f.write(f"Comparison Results for {model_name}\n")
            f.write("=" * 50 + "\n\n")
            
            f.write("Column Changes:\n")
            f.write("-" * 20 + "\n")
            f.write(f"Common columns: {column_changes['common_columns']}\n")
            f.write(f"Main branch only: {column_changes['main_only_columns']}\n")
            f.write(f"Current branch only: {column_changes['current_only_columns']}\n\n")
            
            f.write("Row Counts:\n")
            f.write("-" * 20 + "\n")
            f.write(f"Main branch rows: {statistics['main_rows']}\n")
            f.write(f"Current branch rows: {statistics['current_rows']}\n")
            f.write(f"Difference: {statistics['row_difference']}\n\n")
            
            f.write("Column Statistics:\n")
            f.write("-" * 20 + "\n")
            for col in column_changes['common_columns'].split(', '):
                if col:  # Skip empty strings
                    f.write(f"\n{col}:\n")
                    f.write(f"  Non-null counts - Main: {statistics[f'{col}_main_non_nulls']}, ")
                    f.write(f"Current: {statistics[f'{col}_current_non_nulls']}\n")
                    f.write(f"  Distinct values - Main: {statistics[f'{col}_main_distinct']}, ")
                    f.write(f"Current: {statistics[f'{col}_current_distinct']}\n")
        
        print(f"\nResults saved to: {result_dir}/summary.txt")
        return result_dir
        
    except Exception as e:
        print(f"Error saving results: {e}")
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
        print("Created comparison macro")
        
        # Run models
        print("\nRunning models...")
        subprocess.run(['dbt', 'run', '--models', f"{main_name} {current_name}"], check=True)
        
        # Run comparison
        print("\nComparing versions...")
        result = subprocess.run(
            ['dbt', 'run-operation', 'compare_versions'],
            capture_output=True,
            text=True,
            check=True
        )
        
        # Save results
        save_results(result.stdout, args.output_dir, original_name)
        
    finally:
        # Cleanup
        for path in [main_path, current_path, macro_path]:
            if path and path.exists():
                try:
                    os.remove(path)
                    print(f"Cleaned up temporary file: {path}")
                except Exception as e:
                    print(f"Warning: Could not remove temporary file {path}: {e}")

if __name__ == "__main__":
    main()
