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

def get_mart_tables(project_dir):
    """
    Get the final output tables from dbt-project-evaluator
    """
    mart_patterns = [
        "test_coverage",
        "model_naming",
        "exposure_summary",
        "model_tags",
        "models_summary",
        "source_summary",
        "tests_summary",
        "models_resources"
    ]
    
    try:
        # List all models in the project's marts directory
        result = subprocess.run(
            ['dbt', 'ls', '--resource-type', 'model', '--select', 'dbt_project_evaluator.marts.*'],
            capture_output=True,
            text=True,
            cwd=project_dir
        )
        
        if result.returncode == 0:
            # Split output into lines and clean up
            all_tables = [line.strip() for line in result.stdout.split('\n') if line.strip()]
            
            # Filter for final output tables
            mart_tables = [table for table in all_tables 
                         if any(pattern in table.lower() for pattern in mart_patterns)]
            
            print("\nFound evaluator mart tables:")
            for name in mart_tables:
                print(f"- {name}")
            return mart_tables
        else:
            print("Failed to list models:")
            print(result.stderr)
            return []
            
    except Exception as e:
        print(f"Error getting table names: {e}")
        return []

def get_table_contents(project_dir, table_name):
    """
    Get contents of a table using dbt run-operation
    """
    try:
        # Create a macro to select from the table
        macro_content = f"""
{{% macro get_table_data() %}}
    {{% set query %}}
        select * from {{{{ ref('{table_name}') }}}}
    {{% endset %}}
    
    {{% if execute %}}
        {{% set results = run_query(query) %}}
        {{% set results_json = tojson(results.rows) %}}
        {{% do log(results_json, info=True) %}}
    {{% endif %}}
{{% endmacro %}}
"""
        # Write the macro to a temporary file
        macro_path = Path(project_dir) / 'macros' / 'temp_get_data.sql'
        macro_path.parent.mkdir(exist_ok=True)
        macro_path.write_text(macro_content)
        
        # Run the macro
        result = subprocess.run(
            ['dbt', 'run-operation', 'get_table_data'],
            capture_output=True,
            text=True,
            cwd=project_dir
        )
        
        # Clean up the temporary macro
        macro_path.unlink()
        
        if result.returncode == 0:
            # Find the JSON in the output
            start_idx = result.stdout.find('[')
            if start_idx != -1:
                json_str = result.stdout[start_idx:]
                try:
                    data = json.loads(json_str)
                    return pd.DataFrame(data)
                except json.JSONDecodeError:
                    print(f"Could not parse JSON output for {table_name}")
            else:
                print(f"No data found in output for {table_name}")
        else:
            print(f"Failed to get data for {table_name}")
            print(f"Error: {result.stderr}")
        
    except Exception as e:
        print(f"Error getting table contents: {e}")
    
    return None

def get_evaluation_results(project_dir):
    """
    Extract results for each mart table
    """
    # Get mart table names
    mart_tables = get_mart_tables(project_dir)
    
    if not mart_tables:
        print("No mart tables found!")
        return None
    
    tables = {}
    for full_table_name in mart_tables:
        # Extract the simple table name
        table_name = full_table_name.split('.')[-1]
        print(f"\nFetching results for {table_name}...")
        
        df = get_table_contents(project_dir, table_name)
        if df is not None and not df.empty:
            tables[table_name] = df
    
    return tables

def export_to_csv(tables, output_dir):
    """
    Export each result table to a separate CSV file
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
