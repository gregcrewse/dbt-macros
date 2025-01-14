import subprocess
import json
import pandas as pd
from pathlib import Path
import sys
import os

def query_model(project_dir, model_name):
    """
    Query a single model using a dbt macro
    """
    # Create the macro content
    macro_content = f"""
{{% macro get_data() %}}
    {{% set query %}}
        select * from {{{{ ref('{model_name}') }}}}
    {{% endset %}}
    
    {{% if execute %}}
        {{{{ log(tojson(run_query(query).rows), info=True) }}}}
    {{% endif %}}
{{% endmacro %}}
"""
    
    # Ensure macros directory exists and write macro
    macro_dir = Path(project_dir) / 'macros'
    macro_dir.mkdir(exist_ok=True)
    macro_path = macro_dir / f'get_{model_name}_data.sql'
    
    try:
        with open(macro_path, 'w') as f:
            f.write(macro_content)
        
        # Run the macro
        result = subprocess.run(
            ['dbt', 'run-operation', f'get_data'],
            capture_output=True,
            text=True,
            cwd=project_dir
        )
        
        # Clean up
        macro_path.unlink()
        
        if result.returncode == 0:
            # Look for JSON data in output
            for line in result.stdout.split('\n'):
                if line.strip().startswith('['):
                    try:
                        data = json.loads(line.strip())
                        return pd.DataFrame(data)
                    except json.JSONDecodeError:
                        continue
        else:
            print(f"Error querying {model_name}:")
            print(result.stderr)
        
    except Exception as e:
        print(f"Error processing {model_name}: {e}")
        if macro_path.exists():
            macro_path.unlink()
    
    return None

def get_evaluator_models(project_dir):
    """
    Get list of evaluator output models from manifest
    """
    manifest_path = Path(project_dir) / 'target' / 'manifest.json'
    with open(manifest_path) as f:
        manifest = json.load(f)
    
    models = []
    for node_name, node in manifest['nodes'].items():
        if ('dbt_project_evaluator' in node['package_name'] and 
            node['resource_type'] == 'model' and
            any(x in node['name'] for x in ['coverage', 'model_', 'summary', 'resources'])):
            models.append(node['name'])
    
    return models

def main():
    if len(sys.argv) < 2:
        print("Usage: python script.py <project_directory> [output_directory]")
        sys.exit(1)
    
    project_dir = sys.argv[1]
    output_dir = sys.argv[2] if len(sys.argv) > 2 else 'evaluator_results'
    
    # Convert to absolute paths
    project_dir = os.path.abspath(project_dir)
    output_dir = os.path.abspath(output_dir)
    output_path = Path(output_dir)
    output_path.mkdir(exist_ok=True)
    
    # Run evaluator
    print("Running dbt-project-evaluator...")
    try:
        result = subprocess.run(
            ['dbt', 'run', '--select', 'package:dbt_project_evaluator'],
            check=True,
            cwd=project_dir
        )
        print("Evaluation completed successfully")
    except subprocess.CalledProcessError as e:
        print("Failed to run evaluator")
        sys.exit(1)
    
    # Get list of models
    models = get_evaluator_models(project_dir)
    print("\nFound evaluator models:")
    for model in models:
        print(f"- {model}")
    
    # Query each model and save results
    print("\nCollecting results...")
    for model_name in models:
        print(f"\nProcessing {model_name}...")
        df = query_model(project_dir, model_name)
        if df is not None and not df.empty:
            output_file = output_path / f"{model_name}.csv"
            df.to_csv(output_file, index=False)
            print(f"Exported {model_name} to {output_file}")
        else:
            print(f"No data retrieved for {model_name}")
    
    print("\nDone!")

if __name__ == "__main__":
    main()
