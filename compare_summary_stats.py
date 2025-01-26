import subprocess
import json
import pandas as pd
from pathlib import Path
import sys
import os
from datetime import datetime

def create_comparison_macro(project_dir, model_name):
    """Create the comparison macro file"""
    macro_content = '''
{% macro compare_models(model_name) %}
    {{ log("Starting comparison for model: " ~ model_name, info=True) }}
    
    {% set dev_schema = 'NULL' %}
    {% set uat_schema = 'NULL' %}
    
    {% set query %}
        WITH dev_stats AS (
            SELECT 
                COUNT(*) as row_count
            FROM {{ ref(model_name) }}
        ),
        uat_stats AS (
            SELECT 
                COUNT(*) as row_count
            FROM {{ uat_schema }}.{{ model_name }}
        )
        SELECT 
            '{{ model_name }}'::VARCHAR as model_name,
            'row_count'::VARCHAR as metric_name,
            dev_stats.row_count::VARCHAR as dev_value,
            uat_stats.row_count::VARCHAR as uat_value,
            (uat_stats.row_count - dev_stats.row_count)::VARCHAR as difference,
            CASE 
                WHEN dev_stats.row_count = 0 THEN '0'
                ELSE ROUND(((uat_stats.row_count::FLOAT - dev_stats.row_count) / dev_stats.row_count * 100)::NUMERIC, 2)::VARCHAR
            END as percent_change
        FROM dev_stats, uat_stats
    {% endset %}

    {{ log("Running query...", info=True) }}
    
    {% if execute %}
        {% set results = run_query(query) %}
        {% set row = results.rows[0] %}
        {% set output = {
            "model_name": row[0]|string,
            "metric_name": row[1]|string,
            "dev_value": row[2]|string,
            "uat_value": row[3]|string,
            "difference": row[4]|string,
            "percent_change": row[5]|string
        } %}
        {{ log("RESULTS_START", info=True) }}
        {{ log("=" ~ tojson(output) ~ "=", info=True) }}
        {{ log("RESULTS_END", info=True) }}
    {% endif %}
{% endmacro %}
'''
    
    macro_dir = Path(project_dir) / 'macros'
    macro_dir.mkdir(exist_ok=True)
    macro_path = macro_dir / 'model_comparison.sql'
    
    with open(macro_path, 'w') as f:
        f.write(macro_content)
    
    return macro_path

def run_comparison(project_dir, model_name):
    """Run the comparison macro and return results as a DataFrame"""
    try:
        # Create and write the macro
        macro_path = create_comparison_macro(project_dir, model_name)
        print(f"Created macro file at: {macro_path}")
        
        # Run the macro
        cmd = ['dbt', 'run-operation', 'compare_models', '--args', f'{{"model_name": "{model_name}"}}']
        print(f"Running command: {' '.join(cmd)}")
        
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            cwd=project_dir
        )
        
        # Clean up
        macro_path.unlink()
        
        # Print full output for debugging
        print("\nCommand Output:")
        print("-" * 50)
        print(result.stdout)
        print("-" * 50)
        
        if result.returncode == 0:
            results_data = None
            in_results = False
            json_line = None
            
            for line in result.stdout.split('\n'):
                print(f"Processing line: {line}")
                if "RESULTS_START" in line:
                    in_results = True
                elif "RESULTS_END" in line:
                    in_results = False
                elif in_results and "{" in line:
                    # Extract the JSON part of the line
                    try:
                        # Find the start of the JSON object
                        json_start = line.find('{')
                        json_line = line[json_start:]
                        print(f"Attempting to parse JSON: {json_line}")
                        results_data = json.loads(json_line)
                        print(f"Successfully parsed JSON data: {results_data}")
                        df = pd.DataFrame([results_data])
                        return df
                    except json.JSONDecodeError as e:
                        print(f"JSON parsing error: {e}")
                        print(f"Problematic line: {json_line}")
                    except Exception as e:
                        print(f"Error processing line: {e}")
                        print(f"Line content: {line}")
        else:
            print(f"Command failed with return code: {result.returncode}")
            print("Error output:")
            print(result.stderr)
        
    except Exception as e:
        print(f"Error: {str(e)}")
        if 'macro_path' in locals() and macro_path.exists():
            macro_path.unlink()
    
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
        
        df.to_csv(output_file, index=False)
        print(f"\nResults saved to: {output_file}")
        
        # Print summary
        print("\nComparison Summary:")
        print(f"DEV rows: {df['dev_value'].iloc[0]}")
        print(f"UAT rows: {df['uat_value'].iloc[0]}")
        print(f"Difference: {df['difference'].iloc[0]}")
        print(f"Percent Change: {df['percent_change'].iloc[0]}%")
    else:
        print("No comparison results generated. Please check the model name and permissions.")

if __name__ == "__main__":
    main()
