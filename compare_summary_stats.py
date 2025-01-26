import subprocess
import json
import pandas as pd
from pathlib import Path
import sys
import os
from datetime import datetime

def create_comparison_macro(project_dir, model_name):
    """Create the comparison macro file"""
    macro_content = """
{% macro compare_models(model_name) %}
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
                WHEN dev_stats.row_count = 0 THEN NULL::VARCHAR
                ELSE ROUND(((uat_stats.row_count::FLOAT - dev_stats.row_count) / dev_stats.row_count * 100)::NUMERIC, 2)::VARCHAR
            END as percent_change
        FROM dev_stats, uat_stats
    {% endset %}

    {% if execute %}
        {% set results = run_query(query) %}
        {% set results_list = [] %}
        {% for row in results %}
            {% do results_list.append({
                "model_name": model_name,
                "metric_name": "row_count",
                "dev_value": row.dev_value,
                "uat_value": row.uat_value,
                "difference": row.difference,
                "percent_change": row.percent_change
            }) %}
        {% endfor %}
        {{ log("COMPARISON_RESULTS_START", info=True) }}
        {{ log(tojson(results_list), info=True) }}
        {{ log("COMPARISON_RESULTS_END", info=True) }}
    {% endif %}
{% endmacro %}
"""
    
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
        print(f"Running comparison...")
        
        # Run the macro
        cmd = ['dbt', 'run-operation', 'compare_models', '--args', f'{{"model_name": "{model_name}"}}']
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            cwd=project_dir
        )
        
        # Clean up
        macro_path.unlink()
        
        if result.returncode == 0:
            # Look for results between markers
            lines = result.stdout.split('\n')
            start_idx = None
            end_idx = None
            
            for i, line in enumerate(lines):
                if 'COMPARISON_RESULTS_START' in line:
                    start_idx = i + 1
                elif 'COMPARISON_RESULTS_END' in line:
                    end_idx = i
                    break
            
            if start_idx is not None and end_idx is not None:
                json_str = lines[start_idx].strip()
                try:
                    data = json.loads(json_str)
                    df = pd.DataFrame(data)
                    return df
                except json.JSONDecodeError as e:
                    print(f"Error parsing JSON: {str(e)}")
                    print(f"JSON string: {json_str}")
            else:
                print("Could not find comparison results in output")
        else:
            print(f"Error running comparison:")
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
    
    print(f"Comparing model: {model_name}")
    df = run_comparison(project_dir, model_name)
    
    if df is not None and not df.empty:
        # Create timestamp for filename
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_file = f"{model_name}_comparison_{timestamp}.csv"
        
        # Save to CSV
        df.to_csv(output_file, index=False)
        print(f"Results saved to: {output_file}")
        
        # Print only significant changes
        changes = df[df['percent_change'].astype(float).abs() > 0]
        if not changes.empty:
            print("\nSignificant changes found:")
            for _, row in changes.iterrows():
                print(f"{row['metric_name']}: {row['percent_change']}% change")
                print(f"  DEV: {row['dev_value']}")
                print(f"  UAT: {row['uat_value']}\n")
    else:
        print("No comparison results generated. Please check the model name and permissions.")

if __name__ == "__main__":
    main()
