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

def get_evaluator_table_names(project_dir):
    """
    Get the actual table names created by dbt-project-evaluator
    """
    try:
        # List all models in the dbt project
        result = subprocess.run(
            ['dbt', 'ls', '--resource-type', 'model', '--select', 'package:dbt_project_evaluator'],
            capture_output=True,
            text=True,
            cwd=project_dir
        )
        
        if result.returncode == 0:
            # Split output into lines and clean up
            table_names = [line.strip() for line in result.stdout.split('\n') if line.strip()]
            print("\nFound evaluator tables:")
            for name in table_names:
                print(f"- {name}")
            return table_names
        else:
            print("Failed to list models:")
            print(result.stderr)
            return []
            
    except Exception as e:
        print(f"Error getting table names: {e}")
        return []

def parse_dbt_show_output(output):
    """
    Parse the output from dbt show command which comes in a table format
    """
    lines = output.strip().split('\n')
    if len(lines) < 3:  # Need at least header, separator, and one data row
        return None
        
    # Get headers from first line
    headers = [col.strip() for col in lines[0].split('|')[1:-1]]
    
    # Process data rows
    data = []
    for line in lines[2:]:  # Skip the separator line
        if '|' not in line:  # Skip any non-data lines
            continue
        row = [cell.strip() for cell in line.split('|')[1:-1]]
        if len(row) == len(headers):
            data.append(row)
    
    # Create DataFrame
    return pd.DataFrame(data, columns=headers)

def get_evaluation_results(project_dir):
    """
    Extract results by running dbt show for each evaluator table
    """
    # Get actual table names from the project
    evaluator_tables = get_evaluator_table_names(project_dir)
    
    if not evaluator_tables:
        print("No evaluator tables found!")
        return None
    
    tables = {}
    for table in evaluator_tables:
        print(f"\nFetching results for {table}...")
        try:
            # First try dbt show
            result = subprocess.run(
                ['dbt', 'show', '--select', table],
                capture_output=True,
                text=True,
                cwd=project_dir
            )
            
            if result.returncode == 0:
                # Parse the table-formatted output
                df = parse_dbt_show_output(result.stdout)
                if df is not None and not df.empty:
                    # Use the table name without the package prefix as the key
                    simple_name = table.split('.')[-1]
                    tables[simple_name] = df
                else:
                    print(f"No data found in output for {table}")
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
