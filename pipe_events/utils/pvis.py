"""Helpers for reading vessel fields from the product vessel info summary (PVIS).

PVIS variants disagree on how they spell the vessel flag. AIS derives
`gfw_best_flag` from the registry and the vessel classification model, so events
read the self-reported flag instead; the VMS pipe 5 PVIS derives `gfw_best_flag`
as COALESCE(reported flag, registry flag, source tenant), which is the field its
events (and the rest of its published datasets) should read. Rather than encode
that per pipeline, the field is a CLI argument. See PIPELINE-4424.
"""

FLAG_FIELD_PARAM = "product_vessel_info_summary_flag_field"
FIELD_PREFIX_PARAM = "product_vessel_info_summary_field_prefix"
DEFAULT_FLAG_FIELD = "mmsi_flag"


def resolve_flag_field(params):
    """Returns the PVIS field the queries must read the vessel flag from.

    Defaults to `<prefix>mmsi_flag`, the field events read before the flag became
    configurable, so a caller that omits the argument keeps its current behaviour
    whichever prefix it passes.
    """
    return params.get(FLAG_FIELD_PARAM) or (
        f"{params[FIELD_PREFIX_PARAM]}{DEFAULT_FLAG_FIELD}"
    )
