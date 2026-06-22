import boto3
from pyiceberg.catalog import load_catalog

catalog = load_catalog('default', **{
    'type': 'rest',
    'uri': 'http://localhost:8181',
    's3.endpoint': 'http://localhost:9000',
    's3.access-key-id': 'minioadmin',
    's3.secret-access-key': 'minioadmin',
    's3.path-style-access': 'true',
})

s3 = boto3.client('s3',
    endpoint_url='http://localhost:9000',
    aws_access_key_id='minioadmin',
    aws_secret_access_key='minioadmin',
)

def get_latest_metadata(bucket, prefix):
    resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    if 'Contents' not in resp:
        return None
    jsons = [
        o for o in resp['Contents']
        if o['Key'].endswith('.metadata.json')
        and 'version-hint' not in o['Key']
    ]
    if not jsons:
        return None
    latest = sorted(jsons, key=lambda x: x['LastModified'], reverse=True)[0]
    return f"s3://lakehouse/{latest['Key']}"

# all silver tables
silver_tables = [
    ('silver.who_gho_silver',           'silver/who_gho_silver'),
    ('silver.who_gho_scd2',             'silver/who_gho_scd2'),
    ('silver.cdc_nndss_silver',         'silver/cdc_nndss_silver'),
    ('silver.owid_covid_silver',        'silver/owid_covid_silver'),
    ('silver.owid_vaccination_silver',  'silver/owid_vaccination_silver'),
    ('silver.fred_unrate_silver',       'silver/fred_unrate_silver'),
    ('silver.fred_cpiaucsl_silver',     'silver/fred_cpiaucsl_silver'),
    ('silver.fred_dgs10_silver',        'silver/fred_dgs10_silver'),
    ('silver.openfda_drug_event_silver','silver/openfda_drug_event_silver'),
]

print('--- Registering silver tables ---')
for table_name, prefix in silver_tables:
    meta = get_latest_metadata('lakehouse', prefix)
    if not meta:
        print(f'  SKIP {table_name} — no metadata found')
        continue
    try:
        catalog.register_table(table_name, meta)
        count = catalog.load_table(table_name).scan().to_arrow().num_rows
        print(f'  OK   {table_name}: {count:,} rows')
    except Exception as e:
        if 'already exists' in str(e).lower():
            count = catalog.load_table(table_name).scan().to_arrow().num_rows
            print(f'  EXISTS {table_name}: {count:,} rows')
        else:
            print(f'  FAIL {table_name}: {e}')
