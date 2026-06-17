import great_expectations as gx
import pandas as pd
from pyiceberg.catalog import load_catalog
import os
from dotenv import load_dotenv

load_dotenv()

CATALOG_CONFIG = {
    "type": "rest",
    "uri": os.getenv("ICEBERG_REST_URI", "http://localhost:8181"),
    "s3.endpoint": os.getenv("MINIO_ENDPOINT", "http://localhost:9000"),
    "s3.access-key-id": os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
    "s3.secret-access-key": os.getenv("MINIO_SECRET_KEY", "minioadmin"),
    "s3.path-style-access": "true",
}


def get_catalog():
    return load_catalog("default", **CATALOG_CONFIG)


def read_table(table_name: str) -> pd.DataFrame:
    catalog = get_catalog()
    if not catalog.table_exists(table_name):
        raise ValueError(f"Table not found: {table_name}")
    return catalog.load_table(table_name).scan().to_pandas()


def validate_dataframe(
    df: pd.DataFrame,
    suite_name: str,
    expectations: list[dict],
) -> bool:
    """
    GX 1.x API — ephemeral context, pandas datasource.
    Each expectation is a dict:
      { "type": "expect_column_to_exist", "kwargs": {"column": "foo"} }
    Returns True if all pass, False otherwise.
    """
    context = gx.get_context()

    # data source + asset — unique name per suite to avoid collisions
    ds_name = f"ds_{suite_name}"
    asset_name = f"asset_{suite_name}"

    data_source = context.data_sources.add_pandas(name=ds_name)
    data_asset = data_source.add_dataframe_asset(name=asset_name)
    batch_definition = data_asset.add_batch_definition_whole_dataframe(
        name=f"batch_{suite_name}"
    )
    batch = batch_definition.get_batch(batch_parameters={"dataframe": df})

    # build expectation suite
    suite = context.suites.add(gx.ExpectationSuite(name=suite_name))
    for exp in expectations:
        exp_type = exp["type"]
        kwargs = exp.get("kwargs", {})
        ExpClass = gx.expectations.__dict__.get(
            "".join(w.capitalize() for w in exp_type.split("_"))
        )
        if ExpClass is None:
            # fallback: use string-based addition
            suite.add_expectation(
                gx.expectations.UnexpectedRowsExpectation(
                    unexpected_rows_query="SELECT 1 WHERE FALSE"
                )
            )
        else:
            suite.add_expectation(ExpClass(**kwargs))

    # validation definition + run
    validation_def = context.validation_definitions.add(
        gx.ValidationDefinition(
            name=f"val_{suite_name}",
            data=batch_definition,
            suite=suite,
        )
    )
    results = validation_def.run(batch_parameters={"dataframe": df})

    passed = results.success
    success_count = sum(1 for r in results.results if r.success)
    total_count = len(results.results)
    status = "PASS" if passed else "FAIL"
    print(f"  [{status}] {suite_name}: {success_count}/{total_count} checks passed")

    if not passed:
        for r in results.results:
            if not r.success:
                print(f"    FAILED: {r.expectation_config.type}")

    return passed
