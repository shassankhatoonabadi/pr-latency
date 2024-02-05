import github
import joblib
import pandas as pd

from common import cleanup_files, connect_github, force_refresh, get_logger, get_path, initialize, tokens

initialize()
logger = get_logger(__file__, modules={"urllib3": "ERROR"})


def fetch_projects():
    token, client = connect_github()
    while True:
        try:
            logger.info("Fetching list of projects")
            projects = [
                project.full_name.lower() for project in client.search_repositories("stars:>19000", sort="stars")
            ]
        except (github.BadCredentialsException, github.RateLimitExceededException):
            token, client = connect_github(token)
        except Exception as exception:
            logger.error(f"Failed fetching list of projects due to {exception}")
        else:
            break
    connect_github(token, done=True)
    return projects


def fetch_metadata(project):
    metadata = {"project": project, "pulls": None, "stars": None, "archived": None, "fork": None}
    token, client = connect_github()
    while True:
        try:
            logger.info(f"{project}: Fetching metadata")
            repository = client.get_repo(project)
            metadata.update(
                {
                    "pulls": repository.get_pulls(state="all").totalCount,
                    "stars": repository.watchers,
                    "archived": repository.archived,
                    "fork": repository.fork,
                }
            )
        except (github.BadCredentialsException, github.RateLimitExceededException):
            token, client = connect_github(token)
        except Exception as exception:
            logger.error(f"{project}: Failed fetching metadata due to {exception}")
        else:
            break
    connect_github(token, done=True)
    return metadata


def export_projects(metadata):
    pd.DataFrame(metadata).sort_values(["pulls", "stars"], ascending=False).to_csv(
        get_path("projects_fetched"), index=False
    )


def main():
    if cleanup_files("projects_fetched", force_refresh()):
        with joblib.Parallel(n_jobs=len(tokens), prefer="threads", verbose=10) as parallel:
            export_projects(parallel(joblib.delayed(fetch_metadata)(project) for project in fetch_projects()))
    else:
        print("Skip fetching projects")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Stop fetching projects")
        exit(1)
