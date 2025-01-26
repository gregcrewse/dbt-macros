# compare_models.py
import subprocess
import json
import pandas as pd
from pathlib import Path
import sys
import os
from datetime import datetime

def create_comparison_macro(project_dir, model_name):
    """Create the comparison macro file"""
    # The macro content should be copied from the previous file
    # and saved to macros/model_comparison.sql
    macro_path = Path(project_dir) / 'macros' / 'model_comparison.sql'
    return macro_path

def format_comparison_results(data):
    """Format the comparison data into a list of records for DataFrame creation"""
    records = []
    
    # Add total row count comparison
    records.append({
        'comparison_type': 'total_rows',
        'column_name': 'N/A',
        'metric': 'row_count',
        'dev_value': data['total_rows']['dev_value'],
        'uat_value': data['total_rows']['uat_value'],
        'difference': data['total_rows']['difference'],
        'percent_change': data['total_rows']['percent_change']
    })
    
    # Add column-level comparisons
    for col_name, col_data in data['columns'].items():
        # Non-null values comparison
        records.append({
            'comparison_type': 'column_stats',
            'column_name': col_name,
            'metric': 'non_null_count',
            'dev_value': col_data['non_null_values']['dev_value'],
            'uat_value': col_data['non_null_values']['uat_value'],
            'difference': col_data['non_null_values']['difference'],
            'percent_change': col_data['non_null_values']['percent_change']
        })
        
        # Unique values comparison
        records.append({
            'comparison_type': 'column_stats',
            'column_name': col_name,
            'metric': 'unique_count',
            'dev_value': col_data['unique_values']['dev_value'],
            'uat_value': col_data['unique_values']['uat_value'],
            'difference': col_data['unique_values']['difference'],
            'percent_change': col_data['unique_values']['percent_change']
        })
    
    # Add renamed columns
    for rename in data['renamed_columns']:
        records.append({
            'comparison_type': 'renamed_column',
            'column_name': rename['dev_name'],
            'metric': 'renamed_to',
            'dev_value': rename['dev_name'],
            'uat_value': rename['uat_name'],
            'difference': 'N/A',
            'percent_change': None
        })
    
    # Add added columns
    for col in data['added_columns']:
        records.append({
            'comparison_type': 'schema_change',
            'column_name': col,
            'metric': 'added_column',
            'dev_value': 'N/A',
            'uat_value': col,
            'difference': 'N/A',
            'percent_change': None
        })
    
    # Add removed columns
    for col in data['removed_columns']:
        records.append({
            'comparison_type': 'schema_change',
            'column_name': col,
            'metric': 'removed_column',
            'dev_value': col,
            'uat_value': 'N/A',
            'difference': 'N/A',
            'percent_change': None
        })
    
    return records

def run_comparison(project_dir, model_name):
    """Run the comparison macro and return results as a DataFrame"""
    try:
        # Create and write the macro
        macro_path = create_comparison_macro(project_dir, model_name)
        print(f"Using macro file at: {macro_path}")
        
        # Run the macro
        cmd = ['dbt', 'run-operation', 'compare_models', '--args', f'{{"model_name": "{model_name}"}}']
        print(f"Running comparison...")
        
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            cwd=project_dir
        )
        
        if result.returncode == 0:
            # Look for results between markers
            in_results = False
            json_data = None
            
            for line in result.stdout.split('\n'):
                if "=" in line:
                    try:
                        # Extract between = signs
                        json_str = line.split('=')[1].strip()
                        data = json.loads(json_str)
                        records = format_comparison_results(data)
                        return pd.DataFrame(records)
                    except json.JSONDecodeError as e:
                        print(f"Error parsing JSON: {str(e)}")
                        print(f"Attempted to parse: {json_str}")
                    except Exception as e:
                        print(f"Error processing results: {str(e)}")
        else:
            print(f"Error running comparison:")
            print(result.stderr)
        
    except Exception as e:
        print(f"Error: {str(e)}")
    
    return None

def print_comparison_summary(df):
    """Print a readable summary of the comparison results"""
    print("\nComparison Summary:")
    print("-" * 50)
    
    # Print total row count comparison
    row_counts = df[df['comparison_type'] == 'total_rows']
    if not row_counts.empty:
        print("\nRow Count Comparison:")
        print(f"  DEV: {row_counts['dev_value'].iloc[0]:,}")
        print(f"  UAT: {row_counts['uat_value'].iloc[0]:,}")
        print(f"  Difference: {row_counts['difference'].iloc[0]:,}")
        if pd.notnull(row_counts['percent_change'].iloc[0]):
            print(f"  Percent Change: {float(row_counts['percent_change'].iloc[0]):.2f}%")
    
    # Schema changes (added/removed columns)
    schema_changes = df[df['comparison_type'] == 'schema_change']
    if not schema_changes.empty:
        print("\nSchema Changes:")
        for _, row in schema_changes.iterrows():
            if row['metric'] == 'added_column':
                print(f"  + Added column: {row['uat_value']}")
            elif row['metric'] == 'removed_column':
                print(f"  - Removed column: {row['dev_value']}")
    
    # Renamed columns
    renamed = df[df['comparison_type'] == 'renamed_column']
    if not renamed.empty:
        print("\nRenamed Columns:")
        for _, row in renamed.iterrows():
            print(f"  {row['dev_value']} → {row['uat_value']}")
    
    # Column statistics changes
    col_stats = df[df['comparison_type'] == 'column_stats']
    if not col_stats.empty:
        significant_changes = col_stats[
            pd.to_numeric(col_stats['percent_change'], errors='coerce').abs() > 0
        ]
        if not significant_changes.empty:
            print("\nSignificant Column Changes:")
            for _, row in significant_changes.iterrows():
                print(f"\n  {row['column_name']} ({row['metric']}):")
                print(f"    DEV: {row['dev_value']}")
                print(f"    UAT: {row['uat_value']}")
                if pd.notnull(row['percent_change']):
                    print(f"    Change: {float(row['percent_change']):.2f}%")

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
    df = run_comparison(project_dir, model_name)
    
    if df is not None and not df.empty:
        # Create timestamp for filename
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_file = f"{model_name}_comparison_{timestamp}.csv"
        
        # Save to CSV
        df.to_csv(output_file, index=False)
        print(f"\nResults saved to: {output_file}")
        
        # Print summary
        print_comparison_summary(df)
    else:
        print("No comparison results generated. Please check the model name and permissions.")

if __name__ == "__main__":
    main()
