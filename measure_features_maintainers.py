import joblib
import pandas as pd

from common import (
    cleanup_files,
    force_refresh,
    get_logger,
    get_path,
    import_dataset,
    import_patches,
    import_pulls,
    initialize,
    processed,
)

initialize()


def export_features_maintainers(project, features):
    pd.DataFrame(features).to_csv(get_path("features_maintainers", project), index=False)


def measure_features_maintainers(project):
    logger = get_logger(__file__)
    logger.info(f"{project}: Measuring features maintainers")
    dataset = import_dataset(project)
    pulls = import_pulls(project)
    patches = import_patches(project)
    features_all = []
    for pull_number in dataset.index.unique("pull_number"):
        timeline = dataset.query("pull_number == @pull_number")
        pulled = timeline.query("event == 'pulled'")
        contributor = pulled["actor"].iat[0]
        is_maintainer = pulled["is_maintainer"].iat[0]
        is_bot = pulled["is_bot"].iat[0]
        is_open = pulled["is_open"].iat[0]
        is_closed = pulled["is_closed"].iat[0]
        is_merged = pulled["is_merged"].iat[0]
        opened_at = pulled["opened_at"].iat[0]
        closed_at = pulled["closed_at"].iat[0]
        merged_at = pulled["merged_at"].iat[0]
        maintainer_responded_at = pulled["maintainer_responded_at"].iat[0]
        maintainer_responded_by = pulled["maintainer_responded_by"].iat[0]
        maintainer_responded_event = pulled["maintainer_responded_event"].iat[0]
        contributor_responded_at = pulled["contributor_responded_at"].iat[0]
        contributor_responded_event = pulled["contributor_responded_event"].iat[0]
        resolved_at = pulled["resolved_at"].iat[0]
        resolved_by = pulled["resolved_by"].iat[0]
        maintainer_latency = pulled["maintainer_latency"].iat[0]
        contributor_latency = pulled["contributor_latency"].iat[0]
        features = {
            # Identifiers
            "project": project,
            "pull_number": pull_number,
            "contributor": contributor,
            "is_maintainer": is_maintainer,
            "is_bot": is_bot,
            "is_open": is_open,
            "is_closed": is_closed,
            "is_merged": is_merged,
            "opened_at": opened_at,
            "closed_at": closed_at,
            "merged_at": merged_at,
            "maintainer_responded_at": maintainer_responded_at,
            "maintainer_responded_by": maintainer_responded_by,
            "maintainer_responded_event": maintainer_responded_event,
            "contributor_responded_at": contributor_responded_at,
            "contributor_responded_event": contributor_responded_event,
            "resolved_at": resolved_at,
            "resolved_by": resolved_by,
            "maintainer_latency": maintainer_latency,
            "contributor_latency": contributor_latency,
        }
        if pd.notna(maintainer_latency):
            events = dataset.query("time < @opened_at")
            pr_hour = opened_at.hour
            pr_day = opened_at.isoweekday()
            title = pulls.loc[pull_number, "title"]
            body = pulls.loc[pull_number, "body"]
            pr_description = (len(title.split()) if pd.notna(title) else 0) + (
                len(body.split()) if pd.notna(body) else 0
            )
            commits = timeline.query("event == 'committed' and time <= @opened_at")
            pr_commits = len(commits)
            pr_changed_lines = 0
            pr_changed_files = 0
            for sha in commits["sha"]:
                if not (patch := patches.query("pull_number == @pull_number and sha == @sha")).empty:
                    pr_changed_lines += patch["added_lines"].iat[0] + patch["deleted_lines"].iat[0]
                    pr_changed_files += patch["changed_files"].iat[0]
            past_pulled = events.query("event == 'pulled'")
            pulled_contributor = past_pulled.query("actor == @contributor")
            contributor_pulls = len(pulled_contributor)
            contributor_open_pulls = len(pulled_contributor.query("is_open or resolved_at >= @opened_at"))
            contributor_acceptance_rate = (
                len(pulled_contributor.query("merged_at < @opened_at")) / contributor_pulls if contributor_pulls else 0
            )
            contributor_median_latency = pulled_contributor.query("contributor_responded_at < @opened_at")[
                "contributor_latency"
            ].median()
            contributor_median_latency = contributor_median_latency if pd.notna(contributor_median_latency) else 0
            last = opened_at - pd.DateOffset(months=3)  # noqa: F841
            project_pulls = len(past_pulled.query("opened_at >= @last"))
            project_open_pulls = len(past_pulled.query("is_open or resolved_at >= @opened_at"))
            events_last = events.query("time >= @last")
            project_maintainers = events_last.query("is_maintainer and not is_bot")["actor"].nunique()
            project_community = events_last.query("not is_maintainer and not is_bot")["actor"].nunique()
            project_median_latency = past_pulled.query(
                "maintainer_responded_at < @opened_at and maintainer_responded_at >= @last"
            )["maintainer_latency"].median()
            project_median_latency = project_median_latency if pd.notna(project_median_latency) else 0
            features.update(
                {
                    # PR Features
                    "pr_hour": pr_hour,
                    "pr_day": pr_day,
                    "pr_description": pr_description,
                    "pr_commits": pr_commits,
                    "pr_changed_lines": pr_changed_lines,
                    "pr_changed_files": pr_changed_files,
                    # Contributor Features
                    "contributor_pulls": contributor_pulls,
                    "contributor_open_pulls": contributor_open_pulls,
                    "contributor_acceptance_rate": contributor_acceptance_rate,
                    "contributor_median_latency": contributor_median_latency,
                    # Project Features
                    "project_pulls": project_pulls,
                    "project_open_pulls": project_open_pulls,
                    "project_maintainers": project_maintainers,
                    "project_community": project_community,
                    "project_median_latency": project_median_latency,
                }
            )
        features_all.append(features)
    export_features_maintainers(project, features_all)


def main():
    projects = []
    for project in processed():
        if cleanup_files("features_maintainers", force_refresh(), project):
            projects.append(project)
        else:
            print(f"Skip measuring features maintainers for project {project}")
    if projects:
        with joblib.Parallel(n_jobs=-1, verbose=50) as parallel:
            parallel(joblib.delayed(measure_features_maintainers)(project) for project in projects)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Stop measuring features maintainers")
        exit(1)
