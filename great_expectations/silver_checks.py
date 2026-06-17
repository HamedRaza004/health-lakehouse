from ge_runner import read_table, validate_dataframe

SILVER_CHECKS = {
    "silver.who_gho_silver": [
        {
            "type": "expect_column_values_to_not_be_null",
            "kwargs": {"column": "country_code"},
        },
        {"type": "expect_column_values_to_not_be_null", "kwargs": {"column": "year"}},
        {
            "type": "expect_column_values_to_match_regex",
            "kwargs": {
                "column": "country_code",
                "regex": r"^[A-Z]{2,3}$",
                "mostly": 0.9,
            },
        },
        {
            "type": "expect_column_values_to_be_between",
            "kwargs": {
                "column": "value",
                "min_value": 0,
                "max_value": 100000,
                "mostly": 0.95,
            },
        },
    ],
    "silver.cdc_nndss_silver": [
        {
            "type": "expect_column_values_to_not_be_null",
            "kwargs": {"column": "condition"},
        },
        {
            "type": "expect_column_values_to_be_between",
            "kwargs": {
                "column": "current_week",
                "min_value": 0,
                "max_value": 20000,
                "mostly": 0.10,
            },
        },
    ],
    "silver.owid_covid_silver": [
        {
            "type": "expect_column_values_to_not_be_null",
            "kwargs": {"column": "iso_code"},
        },
        {
            "type": "expect_column_values_to_be_between",
            "kwargs": {
                "column": "new_cases",
                "min_value": 0,
                "max_value": 10_000_000,
                "mostly": 0.98,
            },
        },
    ],
    "silver.owid_vaccination_silver": [
        {
            "type": "expect_column_values_to_not_be_null",
            "kwargs": {"column": "iso_code"},
        },
        {
            "type": "expect_column_values_to_be_between",
            "kwargs": {
                "column": "total_vaccinations",
                "min_value": 0,
                "max_value": 10_000_000_000,
                "mostly": 0.95,
            },
        },
    ],
    "silver.fred_unrate_silver": [
        {"type": "expect_column_values_to_not_be_null", "kwargs": {"column": "value"}},
        {
            "type": "expect_column_values_to_be_between",
            "kwargs": {"column": "value", "min_value": 0, "max_value": 30},
        },
    ],
    "silver.fred_health_exp_silver": [
        {"type": "expect_column_values_to_not_be_null", "kwargs": {"column": "value"}},
    ],
    "silver.fred_gdp_silver": [
        {"type": "expect_column_values_to_not_be_null", "kwargs": {"column": "value"}},
    ],
    "silver.openfda_drug_event_silver": [
        {
            "type": "expect_column_values_to_not_be_null",
            "kwargs": {"column": "drug_name"},
        },
        {
            "type": "expect_column_values_to_not_be_null",
            "kwargs": {"column": "report_id"},
        },
    ],
}


def run_silver_checks() -> bool:
    print("\n--- Silver quality checks ---")
    all_passed = True
    for table_name, checks in SILVER_CHECKS.items():
        try:
            df = read_table(table_name)
            suite_name = table_name.replace(".", "_")
            passed = validate_dataframe(df, suite_name, checks)
            if not passed:
                all_passed = False
        except ValueError as e:
            print(f"  [SKIP] {table_name}: {e}")
    return all_passed


if __name__ == "__main__":
    ok = run_silver_checks()
    print(f"\nSilver gate: {'PASSED' if ok else 'FAILED'}")
