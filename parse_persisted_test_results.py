import json
import time
# Define the markers for the sections you want to grab
markers = [
    "test_results",
    "test_failed_row_counts",
    "enriched_test_results",
    "tables_cache",
    "test_result_rows"
]

# Define the input and output files
input_file = "logs/dbt.log"
output_file = 'dbt_packages/elementary/macros/edr/tests/on_run_end/upload_persisted_test_results.sql'

# Read input from the log file
with open(input_file, "r") as f:
    input_data = f.read()

# Find all dbt run markers
run_markers = []
marker_prefix = "============================== "
for line in input_data.splitlines():
    if marker_prefix in line:
        run_markers.append(line)

# Get the latest run marker
latest_run_marker = run_markers[-1] if run_markers else None

# Initialize the sections content
sections = ["", "", "", "", ""]
print("Parsing test results from : ", latest_run_marker)

if latest_run_marker:
    # Extract the run id from the latest run marker
    run_id = latest_run_marker.split(" | ")[1].strip()

    # Find the start and end indices for the relevant sections within the latest run
    for i, marker in enumerate(markers):
        start_marker = f"____start_{marker}"
        end_marker = f"____end_{marker}"

        # Find the start and end of the section
        start_idx = input_data.find(start_marker, input_data.find(run_id))
        end_idx = input_data.find(end_marker, start_idx)

        if start_idx != -1 and end_idx != -1:
            # Extract and clean the section
            section = input_data[start_idx + len(start_marker):end_idx].strip()
            sections[i] = section

# Generate the macro content
macro_content = """
{% macro upload_persisted_test_results() %}
    {% do elementary.upload_dbt_tests() %}
    {{ elementary.file_log("Handling test results.") }}
    {{ elementary.file_log("Get relation. " ~ elementary.get_elementary_relation('test_result_rows')) }}

"""

macro_content += "    {% set cached_elementary_test_results = " + sections[0] + " %}"
macro_content += "\n"
macro_content += "    \n"
macro_content += "    {% set cached_elementary_test_failed_row_counts = " + sections[1] + " %}"
macro_content += "\n"
macro_content += "    \n"
macro_content += "    {% set elementary_test_results = " + sections[2] + " %}"
macro_content += "\n"
macro_content += "    \n"
macro_content += "    {% set tables_cache = " + sections[3] + " %}"
macro_content += "\n"
macro_content += "    \n"
macro_content += "    {% set test_result_rows = " + sections[4] + " %}"
macro_content += "\n"
macro_content += "    \n"
macro_content += """
    {% set store_result_rows_in_own_table = elementary.get_config_var("store_result_rows_in_own_table") %}

    {% set test_metrics_tables = tables_cache.get("metrics").get("relations") %}
    {% set test_columns_snapshot_tables = tables_cache.get("schema_snapshots") %}
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
    {% do elementary.file_log("Handled test results from cache successfully.") %}
    {% do return('') %}
{% endmacro %}
"""
# Write the macro content to the output file
with open(output_file, "w") as f:
    f.write(macro_content)
print("Prior Test results written to : ", output_file)
