"""Builds the region struct/schema dynamically from pipe-regions' own name -> description
registry, instead of hardcoding the region list.

`parse_regions_to_struct` (assets/bigquery/util.sql.j2) and every event schema's
`regions_mean_position` field used to hardcode the same 13-field region list, which had already
silently drifted out of sync with pipe-regions' own registry (see pipe-regions'
publish-registry command). Reading the registry at run time keeps both in sync automatically --
a newly registered region shows up here without a pipe-events code change.
"""

import copy


def fetch_regions_registry(bq, bq_in_regions_registry):
    """Reads pipe-regions' ``(name, description)`` registry table."""
    query = f"SELECT name, description FROM `{bq_in_regions_registry}` ORDER BY name"
    return [dict(row) for row in bq.fetch_rows(query)]


def regions_mean_position_fields(regions):
    """Builds `regions_mean_position`'s `fields` list from `fetch_regions_registry`'s rows."""
    return [
        {
            "name": region["name"],
            "type": "STRING",
            "mode": "REPEATED",
            "description": region["description"],
        }
        for region in regions
    ]


def with_dynamic_regions_schema(schema, regions):
    """Returns a copy of `schema` with its `regions_mean_position.fields` replaced from
    `regions` (see `fetch_regions_registry`), instead of whatever was hardcoded on disk.
    """
    schema = copy.deepcopy(schema)
    for field in schema:
        if field["name"] == "regions_mean_position":
            field["fields"] = regions_mean_position_fields(regions)
    return schema
