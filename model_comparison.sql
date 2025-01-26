{# macros/model_comparison.sql #}
{% macro compare_models(model_name) %}
    {{ log("Starting comparison for model: " ~ model_name, info=True) }}
    
    {% set dev_schema = 'NULL' %}
    {% set uat_schema = 'NULL' %}
    
    {# Get columns from both environments #}
    {{ log("Getting DEV columns...", info=True) }}
    {% set dev_columns = adapter.get_columns_in_relation(ref(model_name)) %}
    {{ log("Found " ~ dev_columns|length ~ " columns in DEV", info=True) }}
    
    {{ log("Getting UAT relation...", info=True) }}
    {% set uat_relation = adapter.get_relation(database=none, schema=uat_schema, identifier=model_name) %}
    
    {% if uat_relation is none %}
        {{ log("ERROR: Could not find model in UAT schema", info=True) }}
        {{ return(none) }}
    {% endif %}
    
    {{ log("Getting UAT columns...", info=True) }}
    {% set uat_columns = adapter.get_columns_in_relation(uat_relation) %}
    {{ log("Found " ~ uat_columns|length ~ " columns in UAT", info=True) }}
    
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
    {{ log("Generating statistics query...", info=True) }}
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
            dev_stats.total_rows::VARCHAR as dev_total_rows,
            uat_stats.total_rows::VARCHAR as uat_total_rows
            {% for column in dev_columns %}
                {% if column.name in uat_col_map %}
                    ,dev_stats.non_null_{{ column.name }}::VARCHAR as dev_non_null_{{ column.name }}
                    ,uat_stats.non_null_{{ column.name }}::VARCHAR as uat_non_null_{{ column.name }}
                    ,dev_stats.unique_{{ column.name }}::VARCHAR as dev_unique_{{ column.name }}
                    ,uat_stats.unique_{{ column.name }}::VARCHAR as uat_unique_{{ column.name }}
                {% endif %}
            {% endfor %}
        FROM dev_stats, uat_stats
    {% endset %}

    {{ log("Running query:", info=True) }}
    {{ log(stats_query, info=True) }}
    
    {% if execute %}
        {{ log("Executing query...", info=True) }}
        {% set results = run_query(stats_query) %}
        {% set stats_row = results.rows[0] %}
        {{ log("Query executed successfully", info=True) }}
        
        {# Process results into comparison data #}
        {% set comparison_data = {
            'model_name': model_name,
            'total_rows': {
                'dev_value': stats_row.dev_total_rows,
                'uat_value': stats_row.uat_total_rows,
                'difference': (stats_row.uat_total_rows|int - stats_row.dev_total_rows|int)|string,
                'percent_change': ((((stats_row.uat_total_rows|int - stats_row.dev_total_rows|int) / stats_row.dev_total_rows|int) * 100)|round(2))|string if stats_row.dev_total_rows|int > 0 else '0'
            },
            'columns': {},
            'renamed_columns': renamed_columns,
            'added_columns': [col for col in uat_col_map if col not in dev_col_map],
            'removed_columns': [col for col in dev_col_map if col not in uat_col_map]
        } %}
        
        {# Add column-level statistics #}
        {{ log("Processing column statistics...", info=True) }}
        {% for column in dev_columns %}
            {% if column.name in uat_col_map %}
                {% set dev_non_null = stats_row['dev_non_null_' ~ column.name]|int %}
                {% set uat_non_null = stats_row['uat_non_null_' ~ column.name]|int %}
                {% set dev_unique = stats_row['dev_unique_' ~ column.name]|int %}
                {% set uat_unique = stats_row['uat_unique_' ~ column.name]|int %}
                
                {% do comparison_data.columns.update({
                    column.name: {
                        'data_type': column.dtype|string,
                        'non_null_values': {
                            'dev_value': dev_non_null|string,
                            'uat_value': uat_non_null|string,
                            'difference': (uat_non_null - dev_non_null)|string,
                            'percent_change': ((((uat_non_null - dev_non_null) / dev_non_null) * 100)|round(2))|string if dev_non_null > 0 else '0'
                        },
                        'unique_values': {
                            'dev_value': dev_unique|string,
                            'uat_value': uat_unique|string,
                            'difference': (uat_unique - dev_unique)|string,
                            'percent_change': ((((uat_unique - dev_unique) / dev_unique) * 100)|round(2))|string if dev_unique > 0 else '0'
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
