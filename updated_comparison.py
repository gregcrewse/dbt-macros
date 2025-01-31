import os
import sys
import subprocess
import argparse
from pathlib import Path
import datetime
import json
import re

def get_main_branch_content(model_path):
    """Get content of the file from main branch."""
    try:
        result = subprocess.run(
            ['git', 'show', 'main:' + str(model_path)], 
            capture_output=True, 
            text=True,
            check=True
        )
        return result.stdout
    except subprocess.CalledProcessError:
        print(f"Warning: Could not find {model_path} in main branch")
        return None

def create_temp_model(content, changes, original_name, model_dir):
    """Create a temporary copy of the model with changes applied."""
    timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
    temp_name = f"temp_{original_name}_{timestamp}"
    temp_path = model_dir / f"{temp_name}.sql"
    
    for old_str, new_str in changes:
        content = content.replace(old_str, new_str)
    
    content = content.replace(f"ref('{original_name}')", f"ref('{temp_name}')")
    
    with open(temp_path, 'w') as f:
        f.write(content)
    
    return temp_path, temp_name

def get_model_columns(model_name):
    """Get columns from a model using dbt describe."""
    try:
        result = subprocess.run(
            ['dbt', 'describe', '--model', model_name, '--output', 'json'],
            capture_output=True,
            text=True,
            check=True
        )
        data = json.loads(result.stdout)
        # Extract column names from the describe output
        columns = []
        for node in data.get('nodes', {}).values():
            if node.get('name') == model_name:
                columns = list(node.get('columns', {}).keys())
                break
        return columns
    except (subprocess.CalledProcessError, json.JSONDecodeError) as e:
        print(f"Error getting columns for {model_name}: {str(e)}")
        return None

def find_primary_key(model_name, columns):
    """Try to identify the primary key column."""
    # Common primary key patterns
    pk_patterns = [
        r'_id$',
        r'^id$',
        r'_key$',
        r'^key$',
        r'_pk$',
        r'^pk$'
    ]
    
    # First check dbt tests in schema.yml
    try:
        for yml_file in Path('models').rglob('*.yml'):
            with open(yml_file) as f:
                try:
                    config = yaml.safe_load(f)
                    for model in config.get('models', []):
                        if model.get('name') == model_name:
                            for column in model.get('columns', []):
                                if any(test.get('name') == 'unique' for test in column.get('tests', [])):
                                    return column.get('name')
                except yaml.YAMLError:
                    continue
    except Exception:
        pass

    # Then try to identify by common patterns
    for pattern in pk_patterns:
        matches = [col for col in columns if re.search(pattern, col, re.IGNORECASE)]
        if matches:
            return matches[0]
    
    # Default to first column if no PK identified
    return columns[0] if columns else None

def generate_comparison_query(original_model, changed_model):
    """Generate a comprehensive comparison query."""
    # Get columns from both models
    original_columns = get_model_columns(original_model)
    changed_columns = get_model_columns(changed_model)
    
    if not original_columns or not changed_columns:
        print("Error: Could not get columns for models")
        return None

    # Find differences in column sets
    all_columns = set(original_columns) | set(changed_columns)
    only_in_original = set(original_columns) - set(changed_columns)
    only_in_changed = set(changed_columns) - set(original_columns)
    common_columns = set(original_columns) & set(changed_columns)

    # Try to identify primary key
    pk_column = find_primary_key(original_model, original_columns) or find_primary_key(changed_model, changed_columns)

    comparison_query = f"""
    -- Column differences summary
    SELECT
        'Column count summary' as metric,
        {len(original_columns)} as original_column_count,
        {len(changed_columns)} as new_column_count,
        {len(only_in_original)} as columns_removed,
        {len(only_in_changed)} as columns_added;

    -- Columns only in original model
    SELECT 'Columns removed' as change_type, column_name
    FROM (VALUES {', '.join(f"('{col}')" for col in only_in_original)}) t(column_name)
    WHERE column_name != '';

    -- Columns only in changed model
    SELECT 'Columns added' as change_type, column_name
    FROM (VALUES {', '.join(f"('{col}')" for col in only_in_changed)}) t(column_name)
    WHERE column_name != '';

    -- Row count comparison
    WITH original_counts AS (
        SELECT COUNT(*) as count FROM {{{{ ref('{original_model}') }}}}
    ),
    new_counts AS (
        SELECT COUNT(*) as count FROM {{{{ ref('{changed_model}') }}}}
    )
    SELECT
        'Row count summary' as metric,
        o.count as original_row_count,
        n.count as new_row_count,
        n.count - o.count as difference
    FROM original_counts o
    CROSS JOIN new_counts n;
    """

    if pk_column and pk_column in common_columns:
        # Add detailed value comparison for common columns
        comparison_query += f"""
        -- Detailed value comparison
        WITH original_data AS (
            SELECT 
                {pk_column},
                {', '.join(f"MD5(CAST({col} AS VARCHAR)) as orig_{col}" for col in common_columns)}
            FROM {{{{ ref('{original_model}') }}}}
        ),
        new_data AS (
            SELECT 
                {pk_column},
                {', '.join(f"MD5(CAST({col} AS VARCHAR)) as new_{col}" for col in common_columns)}
            FROM {{{{ ref('{changed_model}') }}}}
        )
        SELECT
            'Changed values' as change_type,
            col.column_name,
            COUNT(*) as difference_count
        FROM original_data o
        FULL OUTER JOIN new_data n ON o.{pk_column} = n.{pk_column}
        CROSS JOIN (
            VALUES {', '.join(f"('{col}')" for col in common_columns)}
        ) col(column_name)
        WHERE 
            {' OR '.join(f"orig_{col} != new_{col}" for col in common_columns)}
        GROUP BY col.column_name
        HAVING COUNT(*) > 0;

        -- Sample of differences
        WITH original_data AS (
            SELECT *
            FROM {{{{ ref('{original_model}') }}}}
        ),
        new_data AS (
            SELECT *
            FROM {{{{ ref('{changed_model}') }}}}
        )
        SELECT
            o.{pk_column},
            {', '.join(f'''
            o.{col} as orig_{col},
            n.{col} as new_{col},
            CASE WHEN o.{col} != n.{col} THEN 'CHANGED' ELSE 'SAME' END as {col}_status
            ''' for col in common_columns)}
        FROM original_data o
        FULL OUTER JOIN new_data n ON o.{pk_column} = n.{pk_column}
        WHERE {' OR '.join(f"o.{col} != n.{col}" for col in common_columns)}
        LIMIT 5;
        """

    # Write comparison query to a temporary analysis file
    analysis_path = Path('analyses') / f'compare_{original_model}_{changed_model}.sql'
    analysis_path.parent.mkdir(exist_ok=True)
    
    with open(analysis_path, 'w') as f:
        f.write(comparison_query)
    
    return analysis_path

def main():
    parser = argparse.ArgumentParser(description='Test DBT model changes')
    parser.add_argument('model_path', help='Path to the model to test')
    parser.add_argument('--changes', nargs='+', help='Changes to apply in old:new format')
    parser.add_argument('--against-main', action='store_true',
                        help='Compare against version in main branch')
    parser.add_argument('--original-model', 
                        help='Name of the original model to compare against (useful for new files)')
    
    args = parser.parse_args()
    model_path = Path(args.model_path)
    model_dir = model_path.parent
    
    # Handle original model content
    if args.against_main:
        original_content = get_main_branch_content(model_path)
        if not original_content:
            sys.exit(1)
        original_name = model_path.stem
    elif args.original_model:
        original_name = args.original_model
        possible_paths = [
            model_dir / f"{original_name}.sql",
            Path('models') / f"{original_name}.sql"
        ]
        original_path = next((p for p in possible_paths if p.exists()), None)
        if original_path:
            with open(original_path, 'r') as f:
                original_content = f.read()
        else:
            print(f"Error: Could not find original model {args.original_model}")
            sys.exit(1)
    else:
        with open(model_path, 'r') as f:
            original_content = f.read()
        original_name = model_path.stem
    
    changes = [tuple(change.split(':')) for change in (args.changes or [])]
    
    try:
        # Create temporary models
        temp_original_path, temp_original_name = create_temp_model(
            original_content, [], original_name, model_dir)
        print(f"Created temporary original model: {temp_original_path}")
        
        if args.against_main or args.original_model:
            with open(model_path, 'r') as f:
                changed_content = f.read()
        else:
            changed_content = original_content
            
        temp_changed_path, temp_changed_name = create_temp_model(
            changed_content, changes, original_name, model_dir)
        print(f"Created temporary changed model: {temp_changed_path}")
        
        # Run both models
        subprocess.run(['dbt', 'run', '--models', f"{temp_original_name} {temp_changed_name}"])
        
        # Create and run comparison
        comparison_path = generate_comparison_query(temp_original_name, temp_changed_name)
        print(f"Created comparison analysis: {comparison_path}")
        
        # Run the comparison
        subprocess.run(['dbt', 'compile'])
        print("\nPlease check the compiled SQL in target/compiled/analyses/ for the comparison results")
        
    finally:
        # Cleanup
        if 'temp_original_path' in locals():
            os.remove(temp_original_path)
        if 'temp_changed_path' in locals():
            os.remove(temp_changed_path)
        if 'comparison_path' in locals():
            os.remove(comparison_path)

if __name__ == "__main__":
    main()
