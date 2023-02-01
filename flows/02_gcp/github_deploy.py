from web_to_gcs import etl_web_to_gcs
from prefect.filesystems import GitHub
from prefect.deployments import Deployment


git_block = GitHub.load("github")

github_dep = Deployment.build_from_flow(
    flow=etl_web_to_gcs,
    name="github-flow",
    infrastructure=git_block
)

if __name__ == "__main__":
    github_dep.apply()