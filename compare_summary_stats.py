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
    
    {# Get columns from both environments #}
    {% set dev_columns = adapter.get_columns_in_relation(ref(model_name)) %}
    {% set uat_relation = adapter.get_relation(database=none, schema=uat_schema, identifier=model_name) %}
    {% set uat_columns = adapter.get_columns_in_relation(uat_relation) %}
    
    {# Create maps of column names to data types #}
    {% set dev_col_map = {} %}
    {% set uat_col_map = {} %}
    {% for col in dev_columns %}
        {% do dev_col_map.update({col.name: col.dtype}) %}
    {% endfor %}
    {% for col in uat_columns %}
        {% do uat_col_map.update({col.name: col.dtype}) %}
    {% endfor %}
    
    {# Identify renamed columns based on similar names #}
    {% set renamed_columns = [] %}
    {% for dev_col in dev_columns %}
        {% if dev_col.name not in uat_col_map %}
            {% for uat_col in uat_columns %}
                {% if uat_col.name not in dev_col_map %}
                    {% if dev_col.name.lower().replace('_','') == uat_col.name.lower().replace('_','') %}
                        {% do renamed_columns.append({
                            'dev_name': dev_col.name,
                            'uat_name': uat_col.name,
                            'data_type': dev_col.dtype
                        }) %}
                    {% endif %}
                {% endif %}
            {% endfor %}
        {% endif %}
    {% endfor %}
    
    {# Generate column statistics query #}
    {% set stats_query %}
        WITH dev_stats AS (
            SELECT 
                COUNT(*) as total_rows,
                {% for column in dev_columns %}
                    COUNT({{ column.name }}) as non_null_{{ column.name }},
                    COUNT(DISTINCT {{ column.name }}) as unique_{{ column.name }}
                    {% if not loop.last %},{% endif %}
                {% endfor %}
            FROM {{ ref(model_name) }}
        ),
        uat_stats AS (
            SELECT 
                COUNT(*) as total_rows,
                {% for column in uat_columns %}
                    COUNT({{ column.name }}) as non_null_{{ column.name }},
                    COUNT(DISTINCT {{ column.name }}) as unique_{{ column.name }}
                    {% if not loop.last %},{% endif %}
                {% endfor %}
            FROM {{ uat_schema }}.{{ model_name }}
        )
            SELECT 
                dev_stats.total_rows as dev_total_rows,
                uat_stats.total_rows as uat_total_rows
                {% for column in dev_columns %}
                    {% if column.name in uat_col_map %}
                        ,dev_stats.non_null_{{ column.name }} as dev_non_null_{{ column.name }}
                        ,uat_stats.non_null_{{ column.name }} as uat_non_null_{{ column.name }}
                        ,dev_stats.unique_{{ column.name }} as dev_unique_{{ column.name }}
                        ,uat_stats.unique_{{ column.name }} as uat_unique_{{ column.name }}
                    {% endif %}
                {% endfor %}
            FROM dev_stats, uat_stats
        {% endset %}
    
        {{ log("Running statistics query...", info=True) }}
        
        {% if execute %}
            {% set results = run_query(stats_query) %}
            {% set stats_row = results.rows[0] %}
            
            {# Process results into comparison data #}
            {% set comparison_data = {
                'model_name': model_name,
                'total_rows': {
                    'dev_value': stats_row.dev_total_rows,
                    'uat_value': stats_row.uat_total_rows,
                    'difference': stats_row.uat_total_rows - stats_row.dev_total_rows,
                    'percent_change': (((stats_row.uat_total_rows - stats_row.dev_total_rows) / stats_row.dev_total_rows) * 100)|round(2) if stats_row.dev_total_rows > 0 else 0
                },
                'columns': {},
                'renamed_columns': renamed_columns,
                'added_columns': [col for col in uat_col_map if col not in dev_col_map],
                'removed_columns': [col for col in dev_col_map if col not in uat_col_map]
            } %}
            
            {# Add column-level statistics #}
            {% for column in dev_columns %}
                {% if column.name in uat_col_map %}
                    {% set dev_non_null = stats_row['dev_non_null_' ~ column.name] %}
                    {% set uat_non_null = stats_row['uat_non_null_' ~ column.name] %}
                    {% set dev_unique = stats_row['dev_unique_' ~ column.name] %}
                    {% set uat_unique = stats_row['uat_unique_' ~ column.name] %}
                    
                    {% do comparison_data.columns.update({
                        column.name: {
                            'data_type': column.dtype,
                            'non_null_values': {
                                'dev_value': dev_non_null,
                                'uat_value': uat_non_null,
                                'difference': uat_non_null - dev_non_null,
                                'percent_change': (((uat_non_null - dev_non_null) / dev_non_null) * 100)|round(2) if dev_non_null > 0 else 0
                            },
                            'unique_values': {
                                'dev_value': dev_unique,
                                'uat_value': uat_unique,
                                'difference': uat_unique - dev_unique,
                                'percent_change': (((uat_unique - dev_unique) / dev_unique) * 100)|round(2) if dev_unique > 0 else 0
                            }
                        }
                    }) %}
                {% endif %}
            {% endfor %}
            
            {{ log("RESULTS_START", info=True) }}
            {{ log("=" ~ tojson(comparison_data) ~ "=", info=True) }}
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
            
            for line in result.stdout.split('\n'):
                print(f"Processing line: {line}")
                if "=" in line:
                    try:
                        # Extract between = signs
                        json_str = line.split('=')[1].strip()
                        print(f"Extracted JSON string: {json_str}")
                        
                        # Try to parse the JSON
                        results_data = json.loads(json_str)
                        print(f"Successfully parsed JSON data: {results_data}")
                        
                        # Create DataFrame
                        df = pd.DataFrame([results_data])
                        
                        # Convert percent_change to numeric
                        if 'percent_change' in df.columns:
                            df['percent_change'] = pd.to_numeric(df['percent_change'], errors='coerce')
                        
                        return df
                    except json.JSONDecodeError as e:
                        print(f"JSON parsing error: {e}")
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
