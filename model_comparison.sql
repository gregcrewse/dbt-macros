{# macros/model_comparison.sql #}
{% macro compare_models(model_name) %}
    {{ log("Starting comparison for model: " ~ model_name, info=True) }}
    
    {% set dev_schema = 'NULL' %}
    {% set uat_schema = 'NULL' %}
    {% set database = 'NULL' %}
    
    {# Get columns from both environments #}
    {{ log("Getting DEV columns...", info=True) }}
    {% set dev_columns = adapter.get_columns_in_relation(ref(model_name)) %}
    {{ log("Found " ~ dev_columns|length ~ " columns in DEV", info=True) }}
    
    {{ log("Getting UAT relation...", info=True) }}
    {% set uat_relation = adapter.get_relation(
        database=database,
        schema=uat_schema,
        identifier=model_name
    ) %}
    
    {% if uat_relation is none %}
        {{ log("ERROR: Could not find model in UAT schema", info=True) }}
        {{ return(none) }}
    {% endif %}
    
    {{ log("Getting UAT columns...", info=True) }}
    {% set uat_columns = adapter.get_columns_in_relation(uat_relation) %}
    {{ log("Found " ~ uat_columns|length ~ " columns in UAT", info=True) }}
    
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
            FROM {{ database }}.{{ uat_schema }}.{{ model_name }}
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

    {{ log("Running query...", info=True) }}
    {{ log(stats_query, info=True) }}
    
    {% if execute %}
        {{ log("Executing query...", info=True) }}
        {% set results = run_query(stats_query) %}
        {% set stats_row = results.rows[0] %}
        
        {% set comparison_data = {
            'model_name': model_name,
            'total_rows': {
                'dev_value': stats_row.dev_total_rows,
                'uat_value': stats_row.uat_total_rows,
                'difference': (stats_row.uat_total_rows|int - stats_row.dev_total_rows|int)|string,
                'percent_change': ((((stats_row.uat_total_rows|int - stats_row.dev_total_rows|int) / stats_row.dev_total_rows|int) * 100)|round(2))|string if stats_row.dev_total_rows|int > 0 else '0'
            }
        } %}
        
        {{ log("RESULTS_START", info=True) }}
        {{ log("=" ~ tojson(comparison_data) ~ "=", info=True) }}
        {{ log("RESULTS_END", info=True) }}
    {% endif %}
{% endmacro %}
