import subprocess
import json
import pandas as pd
from pathlib import Path
import sys
import os

def run_dbt_deps(project_dir):
    """
    Run dbt deps and handle any package updates
    """
    try:
        result = subprocess.run(
            ['dbt', 'deps'],
            capture_output=True,
            text=True,
            cwd=project_dir
        )
        return True
    except subprocess.CalledProcessError as e:
        print(f"Error running dbt deps: {e}")
        print(f"Error output: {e.stderr}")
        return False

def run_dbt_evaluator(project_dir):
    """
    Run dbt-project-evaluator on specified project directory and return results
    """
    try:
        # Run parse first
        print("Parsing project...")
        parse_result = subprocess.run(
            ['dbt', 'parse'],
            capture_output=True,
            text=True,
            cwd=project_dir
        )
        
        # Run the evaluator models
        print("Running evaluator models...")
        result = subprocess.run(
            ['dbt', 'run', '--select', 'package:dbt_project_evaluator'],
            capture_output=True,
            text=True,
            cwd=project_dir
        )
        
        if result.returncode != 0:
            print("dbt run failed:")
            print(result.stderr)
            return False
            
        return True
    except subprocess.CalledProcessError as e:
        print(f"Error running dbt: {e}")
        print(f"Error output: {e.stderr}")
        return False
    except Exception as e:
        print(f"Unexpected error: {e}")
        return False

def get_evaluator_models(project_dir):
    """
    Get list of evaluator models by reading target/manifest.json
    """
    try:
        manifest_path = Path(project_dir) / 'target' / 'manifest.json'
        if not manifest_path.exists():
            print("manifest.json not found. Please ensure dbt compile has run.")
            return []
            
        with open(manifest_path) as f:
            manifest = json.load(f)
        
        evaluator_models = []
        for node_name, node in manifest['nodes'].items():
            if (node['resource_type'] == 'model' and 
                'dbt_project_evaluator' in node['package_name'] and
                node.get('config', {}).get('materialized') == 'table'):
                evaluator_models.append(node['name'])
        
        print("\nFound evaluator models:")
        for model in evaluator_models:
            print(f"- {model}")
            
        return evaluator_models
        
    except Exception as e:
        print(f"Error reading manifest: {e}")
        return []

def create_temp_macro(project_dir, table_name):
    """
    Create a temporary macro to query the table
    """
    macro_content = f"""
{{% macro get_table_data() %}}
    {{% set query %}}
        select *
        from {{{{ target.schema }}}}.{table_name}
    {{% endset %}}
    
    {{% if execute %}}
        {{{{ log(tojson(run_query(query).rows), info=True) }}}}
    {{% endif %}}
{{% endmacro %}}
"""
    macro_dir = Path(project_dir) / 'macros'
    macro_dir.mkdir(exist_ok=True)
    
    macro_path = macro_dir / 'temp_get_data.sql'
    with open(macro_path, 'w') as f:
        f.write(macro_content)
    return macro_path

def get_table_data(project_dir, table_name):
    """
    Get the data from a single table
    """
    try:
        # Create temporary macro
        macro_path = create_temp_macro(project_dir, table_name)
        
        # Run the macro
        result = subprocess.run(
            ['dbt', 'run-operation', 'get_table_data'],
            capture_output=True,
            text=True,
            cwd=project_dir
        )
        
        # Clean up
        macro_path.unlink()
        
        if result.returncode == 0:
            # Look for JSON data in the output
            for line in result.stdout.split('\n'):
                if line.strip().startswith('['):
                    try:
                        data = json.loads(line.strip())
                        return pd.DataFrame(data)
                    except json.JSONDecodeError:
                        continue
        else:
            print(f"Error running macro for {table_name}:")
            print(result.stderr)
        
    except Exception as e:
        print(f"Error getting data for {table_name}: {e}")
    
    return None

def get_evaluation_results(project_dir):
    """
    Get results from all evaluator models
    """
    # Get list of evaluator models
    evaluator_models = get_evaluator_models(project_dir)
    
    if not evaluator_models:
        print("No evaluator models found!")
        return None
    
    # Get data from each model
    tables = {}
    for model_name in evaluator_models:
        print(f"\nFetching data from {model_name}...")
        df = get_table_data(project_dir, model_name)
        if df is not None and not df.empty:
            tables[model_name] = df
    
    return tables

def export_to_csv(tables, output_dir):
    """
    Export each table to a CSV file
    """
    output_path = Path(output_dir)
    output_path.mkdir(exist_ok=True)
    
    if not tables:
        print("No tables to export")
        return
        
    for table_name, df in tables.items():
        file_path = output_path / f"{table_name}.csv"
        df.to_csv(file_path, index=False)
        print(f"Exported {table_name} to {file_path}")

def main():
    if len(sys.argv) < 2:
        print("Usage: python script.py <project_directory> [output_directory]")
        sys.exit(1)
    
    project_dir = sys.argv[1]
    output_dir = sys.argv[2] if len(sys.argv) > 2 else 'evaluator_results'
    
    # Convert to absolute paths
    project_dir = os.path.abspath(project_dir)
    output_dir = os.path.abspath(output_dir)
    
    print(f"Running dbt deps...")
    if not run_dbt_deps(project_dir):
        print("Failed to run dbt deps")
        sys.exit(1)
    
    print(f"Running dbt-project-evaluator on {project_dir}")
    if run_dbt_evaluator(project_dir):
        print("Evaluation completed successfully")
        
        print("Collecting results...")
        tables = get_evaluation_results(project_dir)
        
        if tables:
            export_to_csv(tables, output_dir)
            print(f"\nResults have been exported to {output_dir}/")
        else:
            print("Failed to extract results")
    else:
        print("Evaluation failed")

if __name__ == "__main__":
    main()
