# model_test.py
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

        # Add config block to ensure proper materialization
        config_block = '''{{
    config(
        materialized='table',
        schema=var('schema_override', target.schema)
    )
}}

'''
        # Create the modified content
        modified_content = config_block + content.replace(f"ref('{original_name}')", f"ref('{temp_name}')")
        
        with open(temp_path, 'w') as f:
            f.write(modified_content)
        
        return temp_path, temp_name
        
    except Exception as e:
        print(f"Error creating temporary model: {e}")
        return None, None

def create_comparison_macro(model1_name: str, model2_name: str) -> Path:
    """Create a macro file for model comparison that returns CSV output."""
    # Updated macro: runs the query, converts the results to CSV, and outputs it.
    macro_content = f'''
{{% macro compare_versions() %}}
    {{% set relation1 = ref('{model1_name}') %}}
    {{% set relation2 = ref('{model2_name}') %}}

    {{% set cols1 = adapter.get_columns_in_relation(relation1) %}}
    {{% set cols2 = adapter.get_columns_in_relation(relation2) %}}

    {{% set common_cols = [] %}}
    {{% set version1_only_cols = [] %}}
    {{% set version2_only_cols = [] %}}
    {{% set type_changes = [] %}}

    {{# Find common and unique columns #}}
    {{% for col1 in cols1 %}}
        {{% set col_in_version2 = false %}}
        {{% for col2 in cols2 %}}
            {{% if col1.name|lower == col2.name|lower %}}
                {{% do common_cols.append(col1.name) %}}
                {{% set col_in_version2 = true %}}
                {{% if col1.dtype != col2.dtype %}}
                    {{% do type_changes.append({{
                        'column': col1.name,
                        'main_type': col1.dtype,
                        'current_type': col2.dtype
                    }}) %}}
                {{% endif %}}
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

    {{% set query %}}
        with row_counts as (
            select
                count(*) as main_rows,
                {{% for col in common_cols %}}
                count({{ col }}) as main_{{ col }}_non_null,
                count(distinct {{ col }}) as main_{{ col }}_distinct
                {{% if not loop.last %}},{{% endif %}}
                {{% endfor %}}
            from {{ relation1 }}
        ),
        current_counts as (
            select
                count(*) as current_rows,
                {{% for col in common_cols %}}
                count({{ col }}) as current_{{ col }}_non_null,
                count(distinct {{ col }}) as current_{{ col }}_distinct
                {{% if not loop.last %}},{{% endif %}}
                {{% endfor %}}
            from {{ relation2 }}
        ),
        schema_changes as (
            select
                '{{ '{{ common_cols|join(",") }}' }}' as common_columns,
                '{{ '{{ version1_only_cols|join(",") }}' }}' as removed_columns,
                '{{ '{{ version2_only_cols|join(",") }}' }}' as added_columns,
                '{{ '{{ type_changes|tojson }}' }}' as type_changes
        )
        select
            r.main_rows,
            c.current_rows,
            c.current_rows - r.main_rows as row_difference,
            s.*
            {{% for col in common_cols %}}
            , r.main_{{ col }}_non_null
            , c.current_{{ col }}_non_null
            , r.main_{{ col }}_distinct
            , c.current_{{ col }}_distinct
            {{% endfor %}}
        from row_counts r
        cross join current_counts c
        cross join schema_changes s
    {{% endset %}}

    {{% set results = run_query(query) %}}
    {{% if results is none %}}
        {{% do log("No results returned", info=True) %}}
        No results returned
    {{% else %}}
        {{% set agate_table = results.table %}}
        {{ agate_table.to_csv() }}
    {{% endif %}}
{{% endmacro %}}
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
        # Parse CSV output: split by newlines, then by comma
        lines = [line for line in results_json.splitlines() if line.strip()]
        if not lines or "No results returned" in results_json:
            print("No comparison data found in output")
            return None
        
        # For a quick summary, we'll just use the CSV string as is.
        with open(result_dir / 'summary.csv', 'w') as f:
            f.write(results_json)
        
        print(f"\nResults saved to: {result_dir}/summary.csv")
        return result_dir
        
    except Exception as e:
        print(f"Error saving results: {e}")
        print("Raw output was:")
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
        print("Created comparison macro")
        
        # Run models using the redshift_preprod target, disabling defer so all models are rebuilt.
        # Prefix the temporary model names with '1+' to include immediate upstream dependencies.
        print("\nRunning models in redshift_preprod (with --no-defer and immediate upstream dependencies)...")
        try:
            model_result = subprocess.run(
                ['dbt', 'run', '--models', f"1+{main_name} 1+{current_name}", 
                 '--target', 'redshift_preprod',
                 '--no-defer',
                 '--vars', '{"schema_override": "_uat"}'],
                capture_output=True,
                text=True
            )
            if model_result.returncode != 0:
                print("Error running models:")
                print("\nStandard output:")
                print(model_result.stdout)
                print("\nError output:")
                print(model_result.stderr)
                sys.exit(1)
            print(model_result.stdout)  # Show successful output
            
        except Exception as e:
            print(f"Error executing dbt run command: {str(e)}")
            sys.exit(1)
        
        # Run comparison
        print("\nComparing versions...")
        try:
            compare_result = subprocess.run(
                ['dbt', 'run-operation', 'compare_versions', '--target', 'redshift_preprod'],
                capture_output=True,
                text=True
            )
            if compare_result.returncode != 0:
                print("\nError running comparison:")
                print("\nStandard output:")
                print(compare_result.stdout)
                print("\nError output:")
                print(compare_result.stderr)
                sys.exit(1)
            
            # Save results
            save_results(compare_result.stdout, args.output_dir, original_name)
            
        except Exception as e:
            print(f"Error executing comparison: {str(e)}")
            sys.exit(1)
        
    finally:
        # Cleanup temporary files
        for path in [main_path, current_path, macro_path]:
            if path and path.exists():
                try:
                    os.remove(path)
                    print(f"Cleaned up temporary file: {path}")
                except Exception as e:
                    print(f"Warning: Could not remove temporary file {path}: {e}")

if __name__ == "__main__":
    main()



