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
        cmd = ['dbt', 'run-operation', 'compare_models', '--args', f'{{"model_name": "{model_name}"}}']
        print(f"Running command: {' '.join(cmd)}")
        result = subprocess.run(cmd, capture_output=True, text=True, cwd=project_dir)
        
        if result.returncode == 0:
            for line in result.stdout.split('\n'):
                if "=" in line:
                    try:
                        parts = line.split("=")
                        json_str = "=".join(parts[1:-1]) if len(parts) >= 3 else parts[1]
                        json_str = json_str.strip()
                        json_data = json.loads(json_str)
                        
                        if json_data:
                            records = []
                            records.append({
                                'comparison_type': 'total_rows',
                                'column_name': 'N/A',
                                'metric': 'row_count',
                                'dev_value': json_data['total_rows']['dev_value'],
                                'uat_value': json_data['total_rows']['uat_value'],
                                'difference': json_data['total_rows']['difference'],
                                'percent_change': json_data['total_rows']['percent_change']
                            })
                            for col_name, col_data in json_data.get('columns', {}).items():
                                records.append({
                                    'comparison_type': 'column_stats',
                                    'column_name': col_name.lower(),
                                    'metric': 'non_null_count',
                                    'dev_value': col_data['non_null_values']['dev_value'],
                                    'uat_value': col_data['non_null_values']['uat_value'],
                                    'difference': col_data['non_null_values']['difference'],
                                    'percent_change': col_data['non_null_values']['percent_change']
                                })
                                records.append({
                                    'comparison_type': 'column_stats',
                                    'column_name': col_name.lower(),
                                    'metric': 'unique_count',
                                    'dev_value': col_data['unique_values']['dev_value'],
                                    'uat_value': col_data['unique_values']['uat_value'],
                                    'difference': col_data['unique_values']['difference'],
                                    'percent_change': col_data['unique_values']['percent_change']
                                })
                            for col in json_data.get('added_columns', []):
                                records.append({
                                    'comparison_type': 'schema_change',
                                    'column_name': col.lower(),
                                    'metric': 'added_column',
                                    'dev_value': 'N/A',
                                    'uat_value': col,
                                    'difference': 'N/A',
                                    'percent_change': None
                                })
                            for col in json_data.get('removed_columns', []):
                                records.append({
                                    'comparison_type': 'schema_change',
                                    'column_name': col.lower(),
                                    'metric': 'removed_column',
                                    'dev_value': col,
                                    'uat_value': 'N/A',
                                    'difference': 'N/A',
                                    'percent_change': None
                                })
                            return pd.DataFrame(records)
                    except Exception as e:
                        print(f"Error parsing results: {str(e)}")
                        continue
        else:
            print(f"Command failed with code {result.returncode}: {result.stderr}")
    except Exception as e:
        print(f"Error: {str(e)}")
    return None

def print_comparison_summary(df):
    """Print a readable summary of the comparison results"""
    print("\nComparison Summary:")
    print("-" * 50)
    if not df.empty:
        row_counts = df[df['comparison_type'] == 'total_rows']
        if not row_counts.empty:
            print("\nRow Count Comparison:")
            print(f"  DEV: {row_counts.iloc[0]['dev_value']}")
            print(f"  UAT: {row_counts.iloc[0]['uat_value']}")
            print(f"  Difference: {row_counts.iloc[0]['difference']}")
            print(f"  Percent Change: {row_counts.iloc[0]['percent_change']}%")
        schema_changes = df[df['comparison_type'] == 'schema_change']
        if not schema_changes.empty:
            print("\nSchema Changes:")
            for _, row in schema_changes.iterrows():
                if row['metric'] == 'added_column':
                    print(f"  + Added column: {row['uat_value']}")
                elif row['metric'] == 'removed_column':
                    print(f"  - Removed column: {row['dev_value']}")
    else:
        print("No changes detected.")

def main():
    if len(sys.argv) < 3:
        print("Usage: python compare_models.py <project_directory> <model_name>")
        sys.exit(1)
    project_dir = os.path.abspath(sys.argv[1])
    model_name = sys.argv[2]
    df = run_comparison(project_dir, model_name)
    if df is not None:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_file = f"{model_name}_comparison_{timestamp}.csv"
        df.to_csv(output_file, index=False)
        print(f"Results saved to: {output_file}")
        print_comparison_summary(df)

if __name__ == "__main__":
    main()
