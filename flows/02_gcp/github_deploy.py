
from prefect.filesystems import GitHub

block = GitHub(
    repository="https://github.com/VideetM/prefect-zoomcamp",

)
block.get_directory("flows") # specify a subfolder of repo
block.save("dev")


# prefect deployment build flows/02_gcp/etl_web_to_gcs.py:etl_web_to_gcs --name github_deploy --tag dev -sb github/dev -a

