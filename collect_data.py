import github
import joblib
import requests

from common import (
    cleanup_files,
    connect_github,
    force_refresh,
    get_logger,
    get_path,
    initialize,
    open_checkpoint,
    open_commits,
    open_metadata,
    open_patches_raw,
    open_pulls_raw,
    open_timelines_raw,
    tocollect,
    tokens,
)

initialize()


def delete_pull(databases, pull):
    if not isinstance(databases, list):
        databases = [databases]
    for database in databases:
        try:
            del database[pull]
        except KeyError:
            pass


def collect_data(project):
    logger = get_logger(__file__, modules={"sqlitedict": "WARNING", "urllib3": "ERROR"})
    get_path("directory", project).mkdir(parents=True, exist_ok=True)
    checkpoint = open_checkpoint(project)
    pulls = open_pulls_raw(project)
    timelines = open_timelines_raw(project)
    commits = open_commits(project)
    patches = open_patches_raw(project)
    metadata = open_metadata(project)
    if checkpoint.get("last") is None:
        checkpoint["last"] = 0
        checkpoint["exclude"] = []
    else:
        logger.info(f"{project}: Last collected data is for pull request {checkpoint.get('pull')}")
    token, client = connect_github()
    while True:
        try:
            logger.info(f"{project}: Collecting list of pull requests")
            repository = client.get_repo(project)
            for pull in repository.get_pulls(state="all", direction="asc")[checkpoint["last"] :]:
                if client.rate_limiting[0] <= tokens[token]:
                    raise github.RateLimitExceededException(
                        403, f"Reached custom rate limit for token {token}", headers=None
                    )
                if (pull_number := pull.number) in checkpoint["exclude"]:
                    logger.info(f"{project}: Deleting data for pull request {pull_number}")
                    delete_pull([pulls, timelines, commits, patches], pull_number)
                else:
                    logger.info(f"{project}: Collecting data for pull request {pull_number}")
                    pulls[pull_number] = pull.data
                    timelines[pull_number] = [event.data for event in repository.get_issue(pull_number).get_timeline()]
                    commits[pull_number] = {commit.data["sha"]: commit.data for commit in pull.get_commits()}
                    patches[pull_number] = requests.get(
                        f"https://patch-diff.githubusercontent.com/raw/{project}/pull/{pull_number}.patch"
                    ).text
                checkpoint["pull"] = pull_number
                checkpoint["last"] += 1
        except (github.BadCredentialsException, github.RateLimitExceededException):
            token, client = connect_github(token)
        except github.UnknownObjectException:
            logger.warning(f"{project}: Project does not exist")
            break
        except Exception as exception:
            if (isinstance(exception, github.GithubException) and exception.status == 422) or isinstance(
                exception, requests.exceptions.RetryError
            ):
                logger.warning(f"{project}: Skip collecting data for pull request {pull_number} due to {exception}")
                checkpoint["exclude"] = [pull_number, *checkpoint["exclude"]]
            else:
                logger.error(f"{project}: Failed collecting data due to {exception}")
        else:
            metadata.update(repository.data)
            checkpoint.terminate()
            logger.info(f"{project}: Finished collecting data")
            break
    connect_github(token, done=True)


def main():
    projects = []
    for project in tocollect():
        if (
            cleanup_files(
                ["checkpoint", "pulls_raw", "timelines_raw", "commits", "patches_raw", "metadata"],
                force_refresh(),
                project,
            )
            or get_path("checkpoint", project).exists()
        ):
            projects.append(project)
        else:
            print(f"Skip collecting data for project {project}")
    if projects:
        with joblib.Parallel(n_jobs=len(tokens), prefer="threads", verbose=10) as parallel:
            parallel(joblib.delayed(collect_data)(project) for project in projects)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Stop collecting data")
        exit(1)
