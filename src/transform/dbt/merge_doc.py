"""TODO: Write here."""

import json

search_str = (
    'n = [o("manifest", "manifest.json" + t), o("catalog", "catalog.json" + t)]'
)

with open("target/index.html") as f:
    content_index = f.read()

with open("target/manifest.json") as f:
    json_manifest = json.loads(f.read())

with open("target/catalog.json") as f:
    json_catalog = json.loads(f.read())

with open("target/index2.html", "w") as f:
    new_str = (
        "n=[{label: 'manifest', data: "
        + json.dumps(json_manifest)
        + "},{label: 'catalog', data: "
        + json.dumps(json_catalog)
        + "}]"
    )
    new_content = content_index.replace(search_str, new_str)
    f.write(new_content)
