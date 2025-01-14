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
        # First, run dbt parse to ensure project is compiled
        parse_result = subprocess.run(
            ['dbt', 'parse'],
            capture_output=True,
            text=True,
            cwd=project_dir
        )
        
        if parse_result.returncode != 0:
            print("dbt parse failed:")
            print(parse_result.stderr)
            return False

        # Then run the evaluator
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

def query_data_from_db():
    """
    Query the results directly from the data warehouse
    You'll need to modify this based on your data warehouse
    """
    try:
        # This assumes you're using dbt's default target schema
        evaluator_tables = [
            'exposures_summary',
            'model_naming',
            'model_tags',
            'models_resources',
            'models_summary',
            'sources_summary',
            'test_coverage',
            'tests_summary'
        ]
        
        tables = {}
        for table in evaluator_tables:
            # Use dbt's CLI to get the data
            result = subprocess.run(
                ['dbt', 'run-operation', 'get_evaluator_results', '--args', f'{{"table_name": "{table}"}}'],
                capture_output=True,
                text=True
            )
            
            if result.returncode == 0:
                try:
                    # Try to parse the output as JSON
                    data = json.loads(result.stdout)
                    tables[table] = pd.DataFrame(data)
                except:
                    print(f"Could not parse results for {table}")
            else:
                print(f"Failed to get results for {table}")
                
        return tables
    except Exception as e:
        print(f"Error querying results: {e}")
        return None

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
        tables = query_data_from_db()
        
        if tables:
            export_to_csv(tables, output_dir)
            print(f"\nResults have been exported to {output_dir}/")
        else:
            print("Failed to extract results")
    else:
        print("Evaluation failed")

if __name__ == "__main__":
    main()