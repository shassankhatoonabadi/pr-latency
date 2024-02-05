import joblib
import pandas as pd

from common import (
    cleanup_files,
    count_months,
    force_refresh,
    get_logger,
    get_path,
    import_dataset,
    initialize,
    open_metadata,
    processed,
)

initialize()


def postprocess_data(project):
    logger = get_logger(__file__, modules={"sqlitedict": "WARNING"})
    logger.info(f"{project}: Postprocessing data")
    dataset = import_dataset(project)
    metadata = open_metadata(project)
    pulled = dataset.query("event == 'pulled'")
    return {
        "project": project,
        "language": metadata["language"],
        "stars": metadata["watchers"],
        "age": count_months(pd.Timestamp(metadata["created_at"]).tz_localize(None), pulled["time"].max()),
        "contributors": pulled["actor"].nunique(),
        "maintainers": dataset.query("is_maintainer")["actor"].nunique(),
        "bots": dataset.query("is_bot")["actor"].nunique(),
        "bot contributors": pulled.query("is_bot")["actor"].nunique(),
        "bot maintainers": dataset.query("is_bot and is_maintainer")["actor"].nunique(),
        "pulls": len(pulled),
        "open": len(pulled.query("is_open")),
        "closed": len(pulled.query("is_closed")),
        "merged": len(pulled.query("is_merged")),
        "maintainer responded": len(pulled.query("maintainer_latency.notna()")),
        "contributor responded": len(pulled.query("contributor_latency.notna()")),
    }


def export_statistics(statistics):
    pd.DataFrame(statistics).to_csv(get_path("statistics"), index=False)


def main():
    if cleanup_files("statistics", force_refresh()):
        with joblib.Parallel(n_jobs=-1, verbose=50) as parallel:
            export_statistics(parallel(joblib.delayed(postprocess_data)(project) for project in processed()))
    else:
        print("Skip postprocessing data")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Stop postprocessing data")
        exit(1)
