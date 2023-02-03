from prefect.infrastructure.docker import DockerContainer

# alternative to creating DockerContainer block in the UI
docker_block = DockerContainer(
    block_name="ingest-taxi", # not sure how to set block name; defaulted to zoom
    name="ingest-taxi",
    # image="discdiver/prefect:zoom",  # insert your image here
    env={
        "EXTRA_PIP_PACKAGES": "gcsfs pandas pandas-gbq pyarrow prefect_gcp",
    },
    image_pull_policy="ALWAYS",
    auto_remove=True,
    _block_document_name="ingest-taxi",
)

docker_block.save("zoom", overwrite=True)