from who_silver  import transform_who_to_silver
from cdc_silver  import transform_cdc_to_silver
from owid_silver import transform_owid_to_silver
from fred_silver import transform_fred_to_silver
from fda_silver  import transform_fda_to_silver
from scd2        import build_scd2_table

def run_all_silver_transforms():
    print("\n=== WHO GHO ===")
    transform_who_to_silver()

    print("\n=== CDC NNDSS ===")
    transform_cdc_to_silver()

    print("\n=== OWID ===")
    transform_owid_to_silver()

    print("\n=== FRED (3 series) ===")
    transform_fred_to_silver()

    print("\n=== OpenFDA ===")
    transform_fda_to_silver()

    print("\n=== SCD2 (WHO) ===")
    build_scd2_table(
        source_table="iceberg.silver.who_gho_silver",
        target_table="iceberg.silver.who_gho_scd2",
        key_cols=["indicator_code", "country_code", "sex"],
        value_cols=["value", "low", "high"],
    )

    print("\nAll silver transforms complete.")

if __name__ == "__main__":
    run_all_silver_transforms()