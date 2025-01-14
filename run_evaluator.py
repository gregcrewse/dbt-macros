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

def get_evaluation_results(project_dir):
    """
    Extract results by running dbt show for each evaluator table
    """
    evaluator_tables = [
        'dbt_project_evaluator_exposures_summary',
        'dbt_project_evaluator_model_naming',
        'dbt_project_evaluator_model_tags',
        'dbt_project_evaluator_models_resources',
        'dbt_project_evaluator_models_summary',
        'dbt_project_evaluator_sources_summary',
        'dbt_project_evaluator_test_coverage',
        'dbt_project_evaluator_tests_summary'
    ]
    
    tables = {}
    for table in evaluator_tables:
        print(f"Fetching results for {table}...")
        try:
            # Use dbt show to get the table contents
            result = subprocess.run(
                ['dbt', 'show', '--select', table],
                capture_output=True,
                text=True,
                cwd=project_dir
            )
            
            if result.returncode == 0:
                # Try to parse the JSON output from dbt show
                try:
                    # Find the JSON part in the output
                    json_start = result.stdout.find('[')
                    if json_start != -1:
                        json_data = result.stdout[json_start:]
                        data = json.loads(json_data)
                        # Create a simplified table name without the prefix
                        simple_name = table.replace('dbt_project_evaluator_', '')
                        tables[simple_name] = pd.DataFrame(data)
                    else:
                        print(f"No data found in output for {table}")
                except json.JSONDecodeError as e:
                    print(f"Could not parse JSON for {table}: {e}")
            else:
                print(f"Failed to get results for {table}")
                print(f"Error: {result.stderr}")
        except Exception as e:
            print(f"Error processing {table}: {e}")
    
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
