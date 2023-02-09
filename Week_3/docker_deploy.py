from prefect.deployments import Deployment
from flv_parquet_web_to_gcs import etl_parent_flow
from prefect.infrastructure.docker import DockerContainer

docker_block = DockerContainer.load("zoom-docker")

docker_dep = Deployment.build_from_flow(
    flow=etl_parent_flow,
    name="fhv-flow",
    infrastructure=docker_block,
)


if __name__ == "__main__":
    docker_dep.apply()
