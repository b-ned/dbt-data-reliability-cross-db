# How to Run

1. Make sure you are running elementary v 15.1
2. Place `parse_persisted_test_results.py` in root of elementary package folder (`dbt_packages/elementary/parse_persisted_test_results.py`)
3. Make sure to replace the handle_test_results.sql macro, this persists the test data in the logging. (`dbt_packages/elementary/macros/edr/tests/on_run_end/handle_tests_results.sql`)
4. Run command

```
dbt test --vars '{"persist_elementary_test_results":true}' ;python dbt_packages/elementary/parse_persisted_test_results.py ;
dbt run-operation elementary.upload_persisted_test_results --profile elementary --target prod_dwh
```

5. Run alert
```
edr monitor --slack-token <token> --slack-channel-name <channel> --group-by alert
```
