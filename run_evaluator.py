import subprocess
import json
import pandas as pd
from pathlib import Path
import sys
import os

def get_evaluator_results(project_dir):
    """
    Get results from manifest.json and query each table/view
    """
    try:
        # Read manifest.json
        manifest_path = Path(project_dir) / 'target' / 'manifest.json'
        with open(manifest_path) as f:
            manifest = json.load(f)

        # Get target config
        with open(Path(project_dir) / 'target' / 'compiled' / 'dbt_project.yml') as f:
            target_config = json.load(f)
            print(f"Using schema: {target_config.get('target_schema', 'default schema')}")

        # Find all result models
        result_models = []
        for node_name, node in manifest['nodes'].items():
            if ('dbt_project_evaluator' in node['package_name'] and 
                node['resource_type'] == 'model' and
                any(x in node['name'] for x in ['coverage', 'model_', 'summary', 'resources'])):
                
                # Get the relation info
                model_info = {
                    'name': node['name'],
                    'relation_name': node.get('relation_name'),
                    'schema': node.get('schema'),
                    'database': node.get('database'),
                    'materialized': node.get('config', {}).get('materialized', 'view')
                }
                result_models.append(model_info)

        print("\nFound result models:")
        for model in result_models:
            print(f"- {model['name']} ({model['materialized']})")
            if model.get('relation_name'):
                print(f"  Relation: {model['relation_name']}")

        # Query each model and store results
        results = {}
        for model in result_models:
            print(f"\nFetching data from {model['name']}...")
            
            # Create macro to query the model
            macro_content = f"""
{{% macro get_model_data() %}}
    {{% set query %}}
        select *
        from {{{{ ref('{model['name']}') }}}}
    {{% endset %}}
    
    {{% if execute %}}
        {{{{ log(tojson(run_query(query).rows), info=True) }}}}
    {{% endif %}}
{{% endmacro %}}
"""
            # Write macro
            macro_dir = Path(project_dir) / 'macros'
            macro_dir.mkdir(exist_ok=True)
            macro_path = macro_dir / 'temp_get_data.sql'
            
            try:
                with open(macro_path, 'w') as f:
                    f.write(macro_content)

                # Run macro
                print(f"Running query for {model['name']}...")
                result = subprocess.run(
                    ['dbt', 'run-operation', 'get_model_data'],
                    capture_output=True,
                    text=True,
                    cwd=project_dir
                )

                # Clean up macro
                macro_path.unlink()

                if result.returncode == 0:
                    # Parse output for JSON data
                    json_data = None
                    for line in result.stdout.split('\n'):
                        if line.strip().startswith('['):
                            try:
                                json_data = json.loads(line.strip())
                                break
                            except json.JSONDecodeError:
                                continue
                    
                    if json_data:
                        results[model['name']] = pd.DataFrame(json_data)
                        print(f"Successfully retrieved data from {model['name']}")
                    else:
                        print(f"No data found in {model['name']}")
                        print("Output was:")
                        print(result.stdout[:500])  # Print first 500 chars of output
                else:
                    print(f"Failed to query {model['name']}")
                    print("Error was:")
                    print(result.stderr)

            except Exception as e:
                print(f"Error processing {model['name']}: {e}")
                if macro_path.exists():
                    macro_path.unlink()

        return results

    except Exception as e:
        print(f"Error: {e}")
        return None

def export_to_csv(tables, output_dir):
    """
    Export each result table to a CSV file
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
    
    # First run the evaluator
    print("Running dbt-project-evaluator...")
    try:
        subprocess.run(['dbt', 'run', '--select', 'package:dbt_project_evaluator'], 
                      check=True, 
                      cwd=project_dir)
        print("Evaluation completed successfully")
    except subprocess.CalledProcessError as e:
        print("Failed to run evaluator:")
        print(e.stderr)
        sys.exit(1)
    
    # Get and export results
    print("\nCollecting results...")
    tables = get_evaluator_results(project_dir)
    
    if tables:
        export_to_csv(tables, output_dir)
        print(f"\nResults have been exported to {output_dir}/")
    else:
        print("Failed to extract results")

if __name__ == "__main__":
    main()
