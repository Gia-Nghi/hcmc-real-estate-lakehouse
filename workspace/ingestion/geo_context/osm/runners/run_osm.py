import os

from ingestion.geo_context.osm.registry import OSM_ENTITY_REGISTRY
from ingestion.geo_context.osm.runners._base import collect_by_grid, upload_dataset


def run_entity(entity_name: str) -> None:
    spec = OSM_ENTITY_REGISTRY[entity_name]

    all_elements = collect_by_grid(entity_name, spec["query_builder"])
    parsed = spec["parser"](all_elements)

    upload_dataset(
        folder=spec["folder"],
        entity_type=spec["entity_type"],
        parsed_payload=parsed,
        raw_elements=all_elements,
    )


def main() -> None:
    entity = os.getenv("OSM_ENTITY", "all")

    if entity == "all":
        for entity_name in OSM_ENTITY_REGISTRY:
            run_entity(entity_name)
    else:
        if entity not in OSM_ENTITY_REGISTRY:
            raise ValueError(
                f"Invalid OSM_ENTITY={entity}. "
                f"Valid values: all, {', '.join(OSM_ENTITY_REGISTRY)}"
            )
        run_entity(entity)


if __name__ == "__main__":
    main()