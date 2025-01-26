# compare_models.py
import subprocess
import json
import pandas as pd
from pathlib import Path
import sys
import os
from datetime import datetime

def run_comparison(project_dir, model_name):
    """Run the comparison macro and return results as a DataFrame"""
    try:
        # Run the macro
        cmd = ['dbt', 'run-operation', 'compare_models', '--args', f'{{"model_name": "{model_name}"}}']
        print(f"Running command: {' '.join(cmd)}")
        
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            cwd=project_dir
        )
        
        # Print full output for debugging
        print("\nCommand Output:")
        print("-" * 50)
        print(result.stdout)
        print("-" * 50)
        print("\nError Output:")
        print("-" * 50)
        print(result.stderr)
        print("-" * 50)
        
        if result.returncode == 0:
            for line in result.stdout.split('\n'):
                print(f"Processing line: {line}")
                if "=" in line:
                    try:
                        # Extract between = signs
                        json_str = line.split('=')[1].strip()
                        print(f"Extracted JSON: {json_str}")
                        results_data = json.loads(json_str)
                        print(f"Successfully parsed JSON data: {results_data}")
                        
                        # Create single row for DataFrame
                        df_data = {
                            'model_name': results_data['model_name'],
                            'dev_rows': results_data['total_rows']['dev_value'],
                            'uat_rows': results_data['total_rows']['uat_value'],
                            'difference': results_data['total_rows']['difference'],
                            'percent_change': results_data['total_rows']['percent_change']
                        }
                        return pd.DataFrame([df_data])
                    except json.JSONDecodeError as e:
                        print(f"JSON parsing error: {str(e)}")
                        print(f"Attempted to parse: {json_str}")
                    except Exception as e:
                        print(f"Error processing line: {e}")
                        print(f"Line content: {line}")
        else:
            print(f"Command failed with return code: {result.returncode}")
            print("Error output:")
            print(result.stderr)
        
    except Exception as e:
        print(f"Error: {str(e)}")
        import traceback
        print(traceback.format_exc())
    
    return None

def main():
    if len(sys.argv) < 3:
        print("Usage: python script.py <project_directory> <model_name>")
        sys.exit(1)
    
    project_dir = os.path.abspath(sys.argv[1])
    model_name = sys.argv[2]
    
    # Verify project directory
    if not os.path.exists(project_dir):
        print(f"Error: Project directory does not exist: {project_dir}")
        sys.exit(1)
    
    if not os.path.exists(os.path.join(project_dir, 'dbt_project.yml')):
        print(f"Error: Not a dbt project directory (no dbt_project.yml found)")
        sys.exit(1)
    
    print(f"Comparing model: {model_name}")
    print(f"Project directory: {project_dir}")
    
    df = run_comparison(project_dir, model_name)
    
    if df is not None and not df.empty:
        # Create timestamp for filename
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_file = f"{model_name}_comparison_{timestamp}.csv"
        
        # Save to CSV
        df.to_csv(output_file, index=False)
        print(f"\nResults saved to: {output_file}")
        
        # Print summary
        print("\nComparison Summary:")
        print(f"DEV rows: {df['dev_rows'].iloc[0]}")
        print(f"UAT rows: {df['uat_rows'].iloc[0]}")
        print(f"Difference: {df['difference'].iloc[0]}")
        print(f"Percent Change: {df['percent_change'].iloc[0]}%")
    else:
        print("No comparison results generated. Please check the model name and permissions.")

if __name__ == "__main__":
    main()
