import csv
import re

import joblib
import pandas as pd

from common import (
    cleanup_files,
    force_refresh,
    get_logger,
    get_path,
    initialize,
    lookup_keys,
    open_commits,
    open_patches_raw,
    open_pulls_raw,
    open_timelines_fixed,
    open_timelines_raw,
    toanalyze,
)

initialize()


def fix_committed(timeline, commits):
    events = []
    for event in timeline:
        if event["event"] == "committed":
            event["author"]["login"] = lookup_keys("author.login", commits[event["sha"]])
        events.append(event)
    return events


def fix_referenced(timeline):
    events = []
    for event in timeline:
        if event["event"] == "referenced":
            event["referenced"] = event["url"].split("/")[4:6] == event["commit_url"].split("/")[4:6]
        events.append(event)
    return events


def unpack_line_and_commit_commented(timeline):
    events = []
    for event in timeline:
        if event["event"] in ["line-commented", "commit-commented"]:
            for comment in event["comments"]:
                events.append({"event": event["event"], **comment})
        else:
            events.append(event)
    return events


def insert_pulled(timeline, pull):
    return [{"event": "pulled", **pull}, *timeline]


def identify_actor(timeline):
    events = []
    for event in timeline:
        actor = lookup_keys(["actor.login", "user.login", "author.login"], event)
        event["actor"] = actor.lower() if actor is not None else "ghost"
        events.append(event)
    return events


def identify_time(timeline):
    events = []
    for event in timeline:
        event["time"] = lookup_keys(["created_at", "committer.date", "submitted_at"], event)
        events.append(event)
    return events


def add_pull_and_event_number(timeline):
    events = []
    pull_number = timeline[0]["number"]
    for event_number, event in enumerate(sorted(timeline, key=lambda event: event["time"])):
        event["pull_number"] = pull_number
        event["event_number"] = event_number
        events.append(event)
    return events


def fix_timeline(timeline, pull, commits):
    timeline = fix_committed(timeline, commits)
    timeline = fix_referenced(timeline)
    timeline = unpack_line_and_commit_commented(timeline)
    timeline = insert_pulled(timeline, pull)
    timeline = identify_actor(timeline)
    timeline = identify_time(timeline)
    timeline = add_pull_and_event_number(timeline)
    return timeline


def fix_timelines(project, timelines, pulls, commits):
    fixed = open_timelines_fixed(project)
    for pull in pulls:
        fixed[pull] = fix_timeline(timelines[pull], pulls[pull], commits[pull])
    return fixed


def filter_timelines(timelines):
    rows = []
    for timeline in timelines.values():
        for event in timeline:
            row = {}
            for column in [
                "pull_number",
                "event_number",
                "event",
                "actor",
                "time",
                "state",
                "commit_id",
                "referenced",
                "sha",
            ]:
                row[column] = lookup_keys(column, event)
            rows.append(row)
    return rows


def filter_pulls(pulls):
    rows = []
    for pull in pulls.values():
        row = {}
        for column in ["number", "html_url", "title", "body"]:
            row[column] = lookup_keys(column, pull)
        rows.append(row)
    return rows


def filter_patches(patches):
    changes = []
    for pull_number, patch in patches.items():
        for diff in re.findall(
            (
                r"(?ms)^From \S+ Mon Sep 17 00:00:00 2001$.+?^---$.+?(?=^From \S+ Mon Sep 17 00:00:00 2001$.+?^---$)"
                r"|^From \S+ Mon Sep 17 00:00:00 2001$.+?^---$.+"
            ),
            patch,
        ):
            added_lines = re.search(r"(?m)^ .+?(\d+) insertions?\(\+\)", diff)
            deleted_lines = re.search(r"(?m)^ .+?(\d+) deletions?\(\-\)", diff)
            changed_files = re.search(r"(?m)^ (\d+) files? changed,", diff)
            changes.append(
                {
                    "pull_number": int(pull_number),
                    "sha": re.match(r"(?ms)^From (\S+) Mon Sep 17 00:00:00 2001$.+?^---$", diff).group(1),
                    "added_lines": added_lines.group(1) if added_lines else 0,
                    "deleted_lines": deleted_lines.group(1) if deleted_lines else 0,
                    "changed_files": changed_files.group(1) if changed_files else 0,
                }
            )
    return changes


def export_timelines(project, timelines):
    pd.DataFrame(timelines).sort_values(["pull_number", "event_number"]).to_csv(
        get_path("timelines", project), index=False
    )


def export_pulls(project, pulls):
    pd.DataFrame(pulls).sort_values("number").to_csv(
        get_path("pulls", project), index=False, quoting=csv.QUOTE_ALL, escapechar="\\"
    )


def export_patches(project, patches):
    pd.DataFrame(patches).sort_values(["pull_number", "sha"]).to_csv(get_path("patches", project), index=False)


def preprocess_data(project):
    logger = get_logger(__file__, modules={"sqlitedict": "WARNING"})
    logger.info(f"{project}: Preprocessing data")
    timelines = open_timelines_raw(project)
    pulls = open_pulls_raw(project)
    commits = open_commits(project)
    patches = open_patches_raw(project)
    timelines = fix_timelines(project, timelines, pulls, commits)
    export_timelines(project, filter_timelines(timelines))
    export_pulls(project, filter_pulls(pulls))
    export_patches(project, filter_patches(patches))
    timelines.terminate()


def main():
    projects = []
    for project in toanalyze():
        if cleanup_files(["timelines_fixed", "timelines", "pulls", "patches"], force_refresh(), project):
            projects.append(project)
        else:
            print(f"Skip preprocessing data for project {project}")
    if projects:
        with joblib.Parallel(n_jobs=-1, verbose=50) as parallel:
            parallel(joblib.delayed(preprocess_data)(project) for project in projects)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Stop preprocessing data")
        exit(1)
