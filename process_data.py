import joblib
import numpy as np
import pandas as pd

from common import (
    cleanup_files,
    convert_dtypes,
    force_refresh,
    get_logger,
    get_path,
    import_bots,
    import_timelines,
    initialize,
    preprocessed,
    selected,
)

initialize()


@convert_dtypes
def add_status(timelines):
    def find_status(timeline):
        pulled = timeline.query("event == 'pulled'")
        closed = timeline.query("event == 'closed'")
        timeline = timeline.assign(
            is_open=False,
            is_closed=False,
            is_merged=False,
            opened_at=pulled["time"].iat[0],
            closed_at=pd.NaT,
            merged_at=pd.NaT,
            closed_by=np.nan,
            merged_by=np.nan,
        )
        if pulled["state"].iat[0] == "closed":
            if not (merged := timeline.query("event == 'merged'")).empty:
                timeline["is_merged"] = True
                timeline["merged_at"] = merged["time"].iat[0]
                timeline["merged_by"] = merged["actor"].iat[0]
            elif not (commit_id := closed.query("commit_id.notna()")).empty:
                timeline["is_merged"] = True
                timeline["merged_at"] = commit_id["time"].iat[0]
                timeline["merged_by"] = commit_id["actor"].iat[0]
            elif not (referenced := timeline.query("referenced")).empty:
                timeline["is_merged"] = True
                timeline["merged_at"] = referenced["time"].iat[0]
                timeline["merged_by"] = referenced["actor"].iat[0]
            else:
                timeline["is_closed"] = True
                if not closed.empty:
                    timeline["closed_at"] = closed["time"].iat[-1]
                    timeline["closed_by"] = closed["actor"].iat[-1]
        else:
            timeline["is_open"] = True
        return timeline

    timelines = timelines.groupby("pull_number", group_keys=False).apply(find_status)
    timelines["resolved_at"] = timelines["merged_at"].fillna(timelines["closed_at"])
    timelines["resolved_by"] = timelines["merged_by"].fillna(timelines["closed_by"])
    return timelines.drop(columns=["state", "commit_id", "referenced"])


@convert_dtypes
def add_contributor(timelines):
    def find_contributor(timeline):
        timeline["is_contributor"] = timeline["actor"] == timeline.query("event == 'pulled'")["actor"].iat[0]
        return timeline

    return timelines.groupby("pull_number", group_keys=False).apply(find_contributor)


@convert_dtypes
def add_maintainer(timelines):
    def find_maintainer(events):
        events = events.assign(is_maintainer=False)
        if (actor := events["actor"].iat[0]) != "ghost":  # noqa: F841
            events.loc[
                events["time"]
                >= pd.concat(
                    [
                        events.query("(merged_by == @actor) or (closed_by == @actor and not is_contributor)")[
                            "resolved_at"
                        ],
                        events.query(
                            "event.isin(['added_to_project', 'converted_note_to_issue', 'deployed',"
                            " 'deployment_environment_changed', 'locked', 'moved_columns_in_project', 'pinned',"
                            " 'removed_from_project', 'review_dismissed', 'transferred', 'unlocked', 'unpinned',"
                            " 'user_blocked']) or (event == 'closed' and not is_contributor)"
                        )["time"],
                    ]
                ).min(),
                "is_maintainer",
            ] = True
        return events

    return timelines.groupby("actor", group_keys=False, observed=True).apply(find_maintainer)


@convert_dtypes
def add_bot(timelines, bots, owners):
    timelines["is_bot"] = (
        timelines["actor"].str.endswith(("bot", "[bot]"))
        | timelines["actor"].isin(bots)
        | timelines["actor"].isin(owners)
    )
    return timelines


@convert_dtypes
def add_maintainer_response(timelines):
    timelines = timelines.assign(is_maintainer_response=False)
    timelines.loc[
        timelines.query(
            "is_maintainer and not is_bot and not is_contributor and (event.isin(['commented', 'reviewed',"
            " 'line-commented', 'commit-commented', 'merged', 'closed', 'reopened']) or (event == 'referenced' and time"
            " == merged_at)) and time > opened_at"
        ).index,
        "is_maintainer_response",
    ] = True
    return timelines


@convert_dtypes
def add_maintainer_latency(timelines):
    def find_maintainer_latency(timeline):
        timeline = timeline.assign(
            maintainer_responded_at=pd.NaT,
            maintainer_responded_by=np.nan,
            maintainer_responded_event=np.nan,
            maintainer_latency=np.nan,
        )
        if not (responses := timeline.query("is_maintainer_response")).empty:
            timeline["maintainer_responded_at"] = responses["time"].iat[0]
            timeline["maintainer_responded_by"] = responses["actor"].iat[0]
            timeline["maintainer_responded_event"] = responses["event"].iat[0]
            timeline["maintainer_latency"] = (
                timeline["maintainer_responded_at"] - timeline["opened_at"]
            ) / np.timedelta64(1, "h")
        return timeline

    return timelines.groupby("pull_number", group_keys=False).apply(find_maintainer_latency)


@convert_dtypes
def add_contributor_response(timelines):
    timelines = timelines.assign(is_contributor_response=False)
    timelines.loc[
        timelines.query(
            "is_contributor and event.isin(['committed', 'head_ref_force_pushed', 'commented', 'reviewed',"
            " 'line-commented', 'commit-commented', 'closed', 'reopened']) and time > maintainer_responded_at"
        ).index,
        "is_contributor_response",
    ] = True
    return timelines


@convert_dtypes
def add_contributor_latency(timelines):
    def find_contributor_latency(timeline):
        timeline = timeline.assign(
            contributor_responded_at=pd.NaT, contributor_responded_event=np.nan, contributor_latency=np.nan
        )
        if not (responses := timeline.query("is_contributor_response")).empty:
            timeline["contributor_responded_at"] = responses["time"].iat[0]
            timeline["contributor_responded_event"] = responses["event"].iat[0]
            timeline["contributor_latency"] = (
                timeline["contributor_responded_at"] - timeline["maintainer_responded_at"]
            ) / np.timedelta64(1, "h")
        return timeline

    return timelines.groupby("pull_number", group_keys=False).apply(find_contributor_latency)


def export_dataset(project, timelines):
    timelines.to_csv(get_path("dataset", project))


def process_data(project, bots, owners):
    logger = get_logger(__file__)
    logger.info(f"{project}: Processing data")
    timelines = import_timelines(project)
    timelines = add_status(timelines)
    timelines = add_contributor(timelines)
    timelines = add_maintainer(timelines)
    timelines = add_bot(timelines, bots, owners)
    timelines = add_maintainer_response(timelines)
    timelines = add_maintainer_latency(timelines)
    timelines = add_contributor_response(timelines)
    timelines = add_contributor_latency(timelines)
    export_dataset(project, timelines)


def main():
    projects = []
    for project in preprocessed():
        if cleanup_files("dataset", force_refresh(), project):
            projects.append(project)
        else:
            print(f"Skip processing data for project {project}")
    if projects:
        with joblib.Parallel(n_jobs=-1, verbose=50) as parallel:
            parallel(
                joblib.delayed(process_data)(
                    project, bots=import_bots().index, owners=[project.split("/")[0] for project in selected()]
                )
                for project in projects
            )


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Stop processing data")
        exit(1)
