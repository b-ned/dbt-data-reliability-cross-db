

{% macro handle_tests_results() %}
    {{ elementary.file_log("Handling test results.") }}
    {{ elementary.file_log("Get relation. " ~ elementary.get_elementary_relation('test_result_rows')) }}
    
    {% set cached_elementary_test_results = elementary.get_cache("elementary_test_results") %}
    {# {% set cached_elementary_test_results = {'test.jaffle_shop.test1': [{'id': '48aa463e-bc83-4894-9e40-310c9330295e.test.jaffle_shop.test1', 'data_issue_id': None, 'test_execution_id': '48aa463e-bc83-4894-9e40-310c9330295e.test.jaffle_shop.test1', 'test_unique_id': 'test.jaffle_shop.test1', 'model_unique_id': None, 'detected_at': '2024-05-17 06:02:45', 'database_name': None, 'schema_name': 'dwh', 'table_name': 'Undefined', 'column_name': None, 'test_type': 'dbt_test', 'test_sub_type': 'singular', 'other': None, 'owners': [], 'tags': [], 'test_results_query': 'select 1', 'test_name': 'test1', 'test_params': {}, 'severity': 'ERROR', 'test_short_name': 'test1', 'test_alias': 'test1', 'result_rows': [{'1': 1}]}]} %} #} #}
    {% set cached_elementary_test_failed_row_counts = elementary.get_cache("elementary_test_failed_row_counts") %}

    {% set store_result_rows_in_own_table = elementary.get_config_var("store_result_rows_in_own_table") %}
    {# {{ elementary.file_log("Get store_result_rows_in_own_table " ~ store_result_rows_in_own_table) }} #}
    
    {% set elementary_test_results = elementary.get_result_enriched_elementary_test_results(cached_elementary_test_results, cached_elementary_test_failed_row_counts, render_result_rows=(not store_result_rows_in_own_table)) %}
    {# {% set elementary_test_results = [{'id': '2c537ac2-b453-4a80-8a53-4649f56507e0.test.jaffle_shop.test1', 'data_issue_id': None, 'test_execution_id': '2c537ac2-b453-4a80-8a53-4649f56507e0.test.jaffle_shop.test1', 'test_unique_id': 'test.jaffle_shop.test1', 'model_unique_id': None, 'detected_at': '2024-05-17 06:49:31', 'database_name': None, 'schema_name': 'dwh', 'table_name': Undefined, 'column_name': None, 'test_type': 'dbt_test', 'test_sub_type': 'singular', 'other': None, 'owners': [], 'tags': [], 'test_results_query': 'select 1', 'test_name': 'test1', 'test_params': {}, 'severity': 'ERROR', 'test_short_name': 'test1', 'test_alias': 'test1', 'status': 'fail', 'failures': 1, 'invocation_id': '2c537ac2-b453-4a80-8a53-4649f56507e0', 'failed_row_count': None, 'test_results_description': 'Got 1 result, configured to fail if != 0'}] %} #}
    
    
    {# {{ elementary.file_log("Get elementary_test_results " ~ elementary_test_results) }} #}

    {% if store_result_rows_in_own_table %}
      {% set test_result_rows = elementary.pop_test_result_rows(elementary_test_results) %}
    {% endif %}
    {{ print(test_result_rows) }}

    {% set tables_cache = elementary.get_cache("tables") %}
    {% set persist_elementary_test_results = elementary.get_config_var("persist_elementary_test_results") %}
    {% if persist_elementary_test_results %}
      {% do elementary.upload_dbt_tests %}
      {{ elementary.file_log("____start_test_results" ~ cached_elementary_test_results ~ "____end_test_results"  )}}
      {{ elementary.file_log("____start_test_failed_row_counts" ~ cached_elementary_test_failed_row_counts ~ "____end_test_failed_row_counts"  )}}
      {{ elementary.file_log("____start_enriched_test_results" ~ elementary_test_results ~ "____end_enriched_test_results"  )}}
      {{ elementary.file_log("____start_tables_cache" ~ tables_cache ~ "____end_tables_cache"  )}}
      {{ elementary.file_log("____start_test_result_rows" ~ test_result_rows ~ "____end_test_result_rows"  )}}
    {% endif %}

    {# {{ elementary.file_log("Get tables_cache " ~ tables_cache) }} #}
    {% set test_metrics_tables = tables_cache.get("metrics").get("relations") %}
    {{ elementary.file_log("Get test_metrics_tables " ~ test_metrics_tables) }}
    {% set test_columns_snapshot_tables = tables_cache.get("schema_snapshots") %}
    {{ elementary.file_log("Get test_columns_snapshot_tables " ~ test_columns_snapshot_tables) }}
    {% set database_name, schema_name = elementary.get_package_database_and_schema('elementary') %}


    {% do elementary.insert_data_monitoring_metrics(database_name, schema_name, test_metrics_tables) %}
    {% do elementary.insert_schema_columns_snapshot(database_name, schema_name, test_columns_snapshot_tables) %}
    {% if test_result_rows %}
      {% set test_result_rows_relation = elementary.get_elementary_relation('test_result_rows') %}
      {% do elementary.insert_rows(test_result_rows_relation, test_result_rows, should_commit=True, chunk_size=elementary.get_config_var('dbt_artifacts_chunk_size')) %}
    {% endif %}
    {% if elementary_test_results %}
      {% set elementary_test_results_relation = elementary.get_elementary_relation('elementary_test_results') %}
      {% do elementary.insert_rows(elementary_test_results_relation, elementary_test_results, should_commit=True, chunk_size=elementary.get_config_var('dbt_artifacts_chunk_size')) %}
    {% endif %}
    {% do elementary.file_log("Handled test results successfully.") %}
    {% do return('') %}
{% endmacro %}

{% macro get_result_enriched_elementary_test_results(cached_elementary_test_results, cached_elementary_test_failed_row_counts, render_result_rows=false) %}
  {% set elementary_test_results = [] %}

  {% for result in results | selectattr('node.resource_type', '==', 'test') %}
    {% set result = elementary.get_run_result_dict(result) %}
    {% set elementary_test_results_rows = cached_elementary_test_results.get(result.node.unique_id) %}
    {% set elementary_test_failed_row_count = cached_elementary_test_failed_row_counts.get(result.node.unique_id) %}

    {# Materializing the test failed and therefore was not added to the cache. #}
    {% if not elementary_test_results_rows %}
      {% set flattened_test = elementary.flatten_test(result.node) %}
      {% set elementary_test_results_rows = [elementary.get_dbt_test_result_row(flattened_test)] %}
    {% endif %}

    {% for elementary_test_results_row in elementary_test_results_rows %}
      {% set failures = elementary_test_results_row.get("failures", result.failures) %}
      {% set status = "pass" if failures == 0 else result.status %}
      {% do elementary_test_results_row.update({'status': status, 'failures': failures, 'invocation_id': invocation_id, 
                                                'failed_row_count': elementary_test_failed_row_count}) %}
      {% do elementary_test_results_row.setdefault('test_results_description', result.message) %}
      {% if render_result_rows %}
        {% do elementary_test_results_row.update({"result_rows": elementary.render_result_rows(elementary_test_results_row.result_rows)}) %}
      {% endif %}
      {% do elementary_test_results.append(elementary_test_results_row) %}
    {% endfor %}
  {% endfor %}

  {% do return(elementary_test_results) %}
{% endmacro %}

{% macro insert_data_monitoring_metrics(database_name, schema_name, test_metrics_tables) %}
    {%- if not test_metrics_tables %}
      {% do return(none) %}
    {% endif %}

    {%- set test_tables_union_query = elementary.union_metrics_query(test_metrics_tables) -%}
    {% if not test_tables_union_query %}
      {% do return(none) %}
    {% endif %}

    {%- set target_relation = elementary.get_elementary_relation('data_monitoring_metrics') -%}
    {{elementary.file_log("Troubleshooting - Target Relation: " ~ target_ralation)}}
    {% if not target_relation %}
      {% do elementary.raise_missing_elementary_models() %}
    {% endif %}

    {%- set temp_relation = elementary.make_temp_view_relation(target_relation) -%}
    {% set insert_query %}
      INSERT INTO {{ target_relation }} (
        id,
        full_table_name,
        column_name,
        metric_name,
        metric_value,
        source_value,
        bucket_start,
        bucket_end,
        bucket_duration_hours,
        updated_at,
        dimension,
        dimension_value,
        metric_properties,
        created_at
      )
      SELECT
        id,
        full_table_name,
        column_name,
        metric_name,
        metric_value,
        source_value,
        bucket_start,
        bucket_end,
        bucket_duration_hours,
        updated_at,
        dimension,
        dimension_value,
        metric_properties,
        {{ elementary.edr_current_timestamp() }} as created_at
      FROM {{ temp_relation }}
    {% endset %}

    {{ elementary.file_log("Inserting metrics into {}.".format(target_relation)) }}
    {%- do elementary.run_query(dbt.create_table_as(True, temp_relation, test_tables_union_query)) %}
    {% do elementary.run_query(insert_query) %}

    {% if not elementary.has_temp_table_support() %}
        {% do adapter.drop_relation(temp_relation) %}
    {% endif %}
{% endmacro %}

{% macro insert_schema_columns_snapshot(database_name, schema_name, test_columns_snapshot_tables) %}
    {%- if not test_columns_snapshot_tables %}
      {% do return(none) %}
    {% endif %}

    {%- set test_tables_union_query = elementary.union_columns_snapshot_query(test_columns_snapshot_tables) -%}
    {% if not test_tables_union_query %}
      {% do return(none) %}
    {% endif %}

    {%- set target_relation = elementary.get_elementary_relation('schema_columns_snapshot') -%}
    {% if not target_relation %}
      {% do elementary.raise_missing_elementary_models() %}
    {% endif %}

    {%- set temp_relation = elementary.make_temp_view_relation(target_relation) -%}
    {% set insert_query %}
      INSERT INTO {{ target_relation }} (
        column_state_id,
        full_column_name,
        full_table_name,
        column_name,
        data_type,
        is_new,
        detected_at,
        created_at
      )
      SELECT
        column_state_id,
        full_column_name,
        full_table_name,
        column_name,
        data_type,
        is_new,
        detected_at,
        {{ elementary.edr_current_timestamp() }} as created_at
      FROM {{ temp_relation }}
    {% endset %}

    {{ elementary.file_log("Inserting schema columns snapshot into {}.".format(target_relation)) }}
    {%- do elementary.run_query(dbt.create_table_as(True, temp_relation, test_tables_union_query)) %}
    {% do elementary.run_query(insert_query) %}

    {% if not elementary.has_temp_table_support() %}
        {% do adapter.drop_relation(temp_relation) %}
    {% endif %}
{% endmacro %}

{% macro pop_test_result_rows(elementary_test_results) %}
  {% set result_rows = [] %}
  {% for test_result in elementary_test_results %}
    {% for result_row in test_result.pop('result_rows', []) %}
      {% do result_rows.append({
        "elementary_test_results_id": test_result.id,
        "detected_at": test_result.detected_at,
        "result_row": result_row
      }) %}
    {% endfor %}
  {% endfor %}
  {% do return(result_rows) %}
{% endmacro %}

{% macro render_result_rows(test_result_rows) %}
  {% set column_size = elementary.get_column_size() %}
  {% if not column_size or (tojson(test_result_rows) | length) < column_size %}
    {% do return(test_result_rows) %}
  {% endif %}
  {% do return(none) %}
{% endmacro %}
