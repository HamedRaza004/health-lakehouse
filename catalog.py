from pyiceberg.catalog import load_catalog

catalog = load_catalog(
    "default",
    **{
        "type": "rest",
        "uri": "http://localhost:8181",
        "s3.endpoint": "http://localhost:9000",
        "s3.access-key-id": "minioadmin",
        "s3.secret-access-key": "minioadmin",
        "s3.path-style-access": "true",
    },
)

catalog.create_namespace("bronze")
catalog.create_namespace("silver")
catalog.create_namespace("gold")

print(catalog.list_namespaces())
# Expected: [('bronze',), ('silver',), ('gold',)]
