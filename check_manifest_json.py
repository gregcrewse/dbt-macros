import json
from pathlib import Path

# Read manifest.json
with open('target/manifest.json') as f:
    manifest = json.load(f)

# Look for evaluator models
for node_name, node in manifest['nodes'].items():
    if ('dbt_project_evaluator' in node['package_name'] and 
        node['resource_type'] == 'model'):
        print("\nModel:", node['name'])
        print("Schema:", node.get('config', {}).get('schema', 'default'))
        print("Materialized:", node.get('config', {}).get('materialized', 'view'))
        print("Database:", node.get('database'))
