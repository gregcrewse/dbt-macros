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
    
    {% set model_relation = ref(model_name) %}
    {% set columns = adapter.get_columns_in_relation(model_relation) %}
    
    WITH dev_stats AS (
        SELECT 
            COUNT(*)::BIGINT as row_count,
            {% for column in columns %}
                COUNT({{ column.name }})::BIGINT as non_null_{{ column.name }}_count,
                COUNT(DISTINCT {{ column.name }})::BIGINT as distinct_{{ column.name }}_count,
                COUNT(DISTINCT CASE WHEN {{ column.name }} IS NOT NULL THEN {{ column.name }} END)::BIGINT as distinct_non_null_{{ column.name }}_count
                {% if not loop.last %},{% endif %}
            {% endfor %}
        FROM {{ model_relation }}
    ),
    uat_stats AS (
        SELECT 
            COUNT(*)::BIGINT as row_count,
            {% for column in columns %}
                COUNT({{ column.name }})::BIGINT as non_null_{{ column.name }}_count,
                COUNT(DISTINCT {{ column.name }})::BIGINT as distinct_{{ column.name }}_count,
                COUNT(DISTINCT CASE WHEN {{ column.name }} IS NOT NULL THEN {{ column.name }} END)::BIGINT as distinct_non_null_{{ column.name }}_count
                {% if not loop.last %},{% endif %}
            {% endfor %}
        FROM {{ uat_schema }}.{{ model_name }}
    )
    SELECT 
        '{{ model_name }}' as model_name,
        'row_count' as metric_name,
        dev_stats.row_count as dev_value,
        uat_stats.row_count as uat_value,
        (uat_stats.row_count - dev_stats.row_count) as difference,
        CASE 
            WHEN dev_stats.row_count = 0 THEN NULL
            ELSE ((uat_stats.row_count::FLOAT - dev_stats.row_count) / dev_stats.row_count * 100)
        END as percent_change
    FROM dev_stats, uat_stats

    {% for column in columns %}
        UNION ALL
        SELECT 
            '{{ model_name }}' as model_name,
            'non_null_{{ column.name }}' as metric_name,
            dev_stats.non_null_{{ column.name }}_count as dev_value,
            uat_stats.non_null_{{ column.name }}_count as uat_value,
            (uat_stats.non_null_{{ column.name }}_count - dev_stats.non_null_{{ column.name }}_count) as difference,
            CASE 
                WHEN dev_stats.non_null_{{ column.name }}_count = 0 THEN NULL
                ELSE ((uat_stats.non_null_{{ column.name }}_count::FLOAT - dev_stats.non_null_{{ column.name }}_count) 
                      / dev_stats.non_null_{{ column.name }}_count * 100)
            END as percent_change
        FROM dev_stats, uat_stats
        
        UNION ALL
        SELECT 
            '{{ model_name }}' as model_name,
            'distinct_{{ column.name }}' as metric_name,
            dev_stats.distinct_{{ column.name }}_count as dev_value,
            uat_stats.distinct_{{ column.name }}_count as uat_value,
            (uat_stats.distinct_{{ column.name }}_count - dev_stats.distinct_{{ column.name }}_count) as difference,
            CASE 
                WHEN dev_stats.distinct_{{ column.name }}_count = 0 THEN NULL
                ELSE ((uat_stats.distinct_{{ column.name }}_count::FLOAT - dev_stats.distinct_{{ column.name }}_count) 
                      / dev_stats.distinct_{{ column.name }}_count * 100)
            END as percent_change
        FROM dev_stats, uat_stats

        UNION ALL
        SELECT 
            '{{ model_name }}' as model_name,
            'distinct_non_null_{{ column.name }}' as metric_name,
            dev_stats.distinct_non_null_{{ column.name }}_count as dev_value,
            uat_stats.distinct_non_null_{{ column.name }}_count as uat_value,
            (uat_stats.distinct_non_null_{{ column.name }}_count - dev_stats.distinct_non_null_{{ column.name }}_count) as difference,
            CASE 
                WHEN dev_stats.distinct_non_null_{{ column.name }}_count = 0 THEN NULL
                ELSE ((uat_stats.distinct_non_null_{{ column.name }}_count::FLOAT - dev_stats.distinct_non_null_{{ column.name }}_count) 
                      / dev_stats.distinct_non_null_{{ column.name }}_count * 100)
            END as percent_change
        FROM dev_stats, uat_stats
    {% endfor %}
    {% if execute %}
        {{ log(tojson(run_query(_dbt_generic_test_sql).rows), info=True) }}
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
        
        # Run the macro
        result = subprocess.run(
            ['dbt', 'run-operation', 'compare_models', '--args', f'{{"model_name": "{model_name}"}}'],
            capture_output=True,
            text=True,
            cwd=project_dir
        )
        
        # Clean up
        macro_path.unlink()
        
        if result.returncode == 0:
            # Parse JSON output from log
            for line in result.stdout.split('\n'):
                if line.strip().startswith('['):
                    try:
                        data = json.loads(line.strip())
                        df = pd.DataFrame(data)
                        return df
                    except json.JSONDecodeError:
                        continue
        else:
            print(f"Error comparing model {model_name}:")
            print(result.stderr)
        
    except Exception as e:
        print(f"Error processing {model_name}: {e}")
        if macro_path.exists():
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
        
        df.to_csv(output_file, index=False)
        print(f"\nResults saved to: {output_file}")
        
        # Print summary to console
        print("\nSummary of changes:")
        summary = df[df['percent_change'].notnull() & (df['percent_change'] != 0)]
        if not summary.empty:
            for _, row in summary.iterrows():
                print(f"\n{row['metric_name']}:")
                print(f"  DEV: {row['dev_value']}")
                print(f"  UAT: {row['uat_value']}")
                print(f"  Change: {row['percent_change']:.2f}%")
        else:
            print("No differences found between DEV and UAT")
    else:
        print("No comparison data generated")

if __name__ == "__main__":
    main()
