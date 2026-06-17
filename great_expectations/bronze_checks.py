from ge_runner import read_table, validate_dataframe

BRONZE_CHECKS = {
    # --------------------------------------------------
    # WHO
    # --------------------------------------------------
    "bronze.who_gho_raw": [
        {"type": "expect_column_to_exist", "kwargs": {"column": "indicator_code"}},
        {"type": "expect_column_to_exist", "kwargs": {"column": "_ingested_at"}},
        {"type": "expect_column_to_exist", "kwargs": {"column": "_source_url"}},
        {
            "type": "expect_column_values_to_not_be_null",
            "kwargs": {"column": "indicator_code"},
        },
        {
            "type": "expect_table_row_count_to_be_between",
            "kwargs": {"min_value": 1000, "max_value": 10000000},
        },
    ],
    # --------------------------------------------------
    # CDC NNDSS
    # --------------------------------------------------
    "bronze.cdc_nndss_raw": [
        {"type": "expect_column_to_exist", "kwargs": {"column": "condition"}},
        {
            "type": "expect_column_values_to_not_be_null",
            "kwargs": {"column": "condition"},
        },
        {
            "type": "expect_table_row_count_to_be_between",
            "kwargs": {"min_value": 100, "max_value": 10000000},
        },
    ],
    # --------------------------------------------------
    # OWID COVID
    # --------------------------------------------------
    "bronze.owid_covid_raw": [
        {"type": "expect_column_to_exist", "kwargs": {"column": "record_json"}},
        {
            "type": "expect_column_values_to_not_be_null",
            "kwargs": {"column": "record_json"},
        },
        {
            "type": "expect_table_row_count_to_be_between",
            "kwargs": {"min_value": 1000, "max_value": 10000000},
        },
    ],
    # --------------------------------------------------
    # OWID VACCINATION
    # --------------------------------------------------
    "bronze.owid_vaccination_raw": [
        {"type": "expect_column_to_exist", "kwargs": {"column": "record_json"}},
        {
            "type": "expect_column_values_to_not_be_null",
            "kwargs": {"column": "record_json"},
        },
        {
            "type": "expect_table_row_count_to_be_between",
            "kwargs": {"min_value": 100, "max_value": 5000000},
        },
    ],
    # --------------------------------------------------
    # FRED UNRATE
    # --------------------------------------------------
    "bronze.fred_unrate_raw": [
        {"type": "expect_column_to_exist", "kwargs": {"column": "record_json"}},
        {
            "type": "expect_column_values_to_not_be_null",
            "kwargs": {"column": "record_json"},
        },
        {
            "type": "expect_table_row_count_to_be_between",
            "kwargs": {"min_value": 10, "max_value": 50000},
        },
    ],
    # --------------------------------------------------
    # FRED CPIAUCSL
    # --------------------------------------------------
    "bronze.fred_cpiaucsl_raw": [
        {"type": "expect_column_to_exist", "kwargs": {"column": "record_json"}},
        {
            "type": "expect_column_values_to_not_be_null",
            "kwargs": {"column": "record_json"},
        },
        {
            "type": "expect_table_row_count_to_be_between",
            "kwargs": {"min_value": 10, "max_value": 50000},
        },
    ],
    # --------------------------------------------------
    # FRED DGS10
    # --------------------------------------------------
    "bronze.fred_dgs10_raw": [
        {"type": "expect_column_to_exist", "kwargs": {"column": "record_json"}},
        {
            "type": "expect_column_values_to_not_be_null",
            "kwargs": {"column": "record_json"},
        },
        {
            "type": "expect_table_row_count_to_be_between",
            "kwargs": {"min_value": 10, "max_value": 50000},
        },
    ],
    # --------------------------------------------------
    # OPENFDA
    # --------------------------------------------------
    "bronze.openfda_drug_event_raw": [
        {"type": "expect_column_to_exist", "kwargs": {"column": "record_json"}},
        {
            "type": "expect_column_values_to_not_be_null",
            "kwargs": {"column": "record_json"},
        },
        {
            "type": "expect_table_row_count_to_be_between",
            "kwargs": {"min_value": 100, "max_value": 5000000},
        },
    ],
}


def run_bronze_checks() -> bool:
    print("\n--- Bronze quality checks ---")

    all_passed = True

    for table_name, checks in BRONZE_CHECKS.items():
        try:
            df = read_table(table_name)

            suite_name = table_name.replace(".", "_")

            passed = validate_dataframe(df, suite_name, checks)

            if not passed:
                all_passed = False

        except Exception as e:
            print(f"  [FAIL] {table_name}: {e}")
            all_passed = False

    return all_passed


if __name__ == "__main__":
    ok = run_bronze_checks()

    print(f"\nBronze gate: {'PASSED' if ok else 'FAILED'}")
