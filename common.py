import argparse
import csv
import functools
import json
import logging
import logging.config
import os
import pathlib
import queue
import sys

import dateutil.relativedelta
import github
import github.GithubObject
import pandas as pd
import sqlitedict
import urllib3
import yaml

sys.setrecursionlimit(1_000_000)
logger = logging.getLogger(__name__)
DATE = pd.Timestamp(2022, 12, 1)
with open(pathlib.Path.home() / "tokens.yaml") as file:
    tokens = yaml.safe_load(file)
tokens_queue = queue.Queue()
for token in tokens:
    tokens_queue.put(token)


@property
def raw_data(self):
    return self._rawData


github.GithubObject.GithubObject.data = raw_data


def initialize(directory=None):
    if directory is None:
        directory = get_path("data")
    directory = pathlib.Path(__file__).parent / directory
    directory.mkdir(parents=True, exist_ok=True)
    os.chdir(directory)


def get_logger(name, level="INFO", modules=None):
    name = pathlib.Path(name).stem
    logging.config.dictConfig(
        {
            "version": 1,
            "formatters": {
                "file": {
                    "format": "{asctime}\t{levelname}\t{name}\t{message}",
                    "datefmt": "%Y-%m-%d %H:%M:%S",
                    "style": "{",
                },
                "stream": {
                    "()": "colorlog.ColoredFormatter",
                    "format": "{blue}{asctime}\t{name}\t{message_log_color}{message}",
                    "datefmt": "%Y-%m-%d %H:%M:%S",
                    "style": "{",
                    "secondary_log_colors": {
                        "message": {
                            "DEBUG": "cyan",
                            "INFO": "green",
                            "WARNING": "yellow",
                            "ERROR": "red",
                            "CRITICAL": "bold_red",
                        }
                    },
                },
            },
            "handlers": {
                "file": {
                    "class": "logging.FileHandler",
                    "formatter": "file",
                    "filename": f"{name}.log",
                },
                "stream": {
                    "class": "colorlog.StreamHandler",
                    "formatter": "stream",
                },
            },
            "root": {
                "level": level,
                "handlers": ["file", "stream"],
            },
            "disable_existing_loggers": False,
        }
    )
    if modules is not None:
        for module, level in modules.items():
            logging.getLogger(module).setLevel(level)
    return logging.getLogger(name)


def connect_github(token=None, done=False):
    if done:
        tokens_queue.put(token)
    else:
        if token is not None:
            tokens_queue.put(token)
        while True:
            try:
                token = tokens_queue.get()
                client = github.Github(
                    token,
                    timeout=20,
                    per_page=100,
                    retry=urllib3.util.retry.Retry(
                        total=None, status=10, status_forcelist=[500, 502, 503, 504], backoff_factor=1
                    ),
                )
                remaining, limit = client.rate_limiting
                if limit < 5000:
                    raise github.BadCredentialsException(401, f"Token {token} is blocked", headers=None)
            except github.BadCredentialsException:
                logger.warning(f"Token {token} is not valid")
            except github.RateLimitExceededException:
                tokens_queue.put(token)
            except Exception as exception:
                logger.error(f"Token {token} is not working due to {exception}")
                tokens_queue.put(token)
            else:
                if remaining > tokens[token]:
                    break
                else:
                    tokens_queue.put(token)
        return token, client


def lookup_keys(attributes, json):
    if not isinstance(attributes, list):
        attributes = [attributes]
    for attribute in attributes:
        if (
            value := functools.reduce(
                lambda dictionary, key: dictionary.get(key) if dictionary else None, attribute.split("."), json
            )
        ) not in [None, ""]:
            return value


def count_months(start, end):
    delta = dateutil.relativedelta.relativedelta(end, start)
    return delta.years * 12 + delta.months


def get_path(file, project=None):
    if project is not None:
        project = project.replace("/", "_").lower()
    directory = f"{project}/"
    files = {
        # Working directory
        "data": "data/",
        # Generated in fetch_projects.py
        "projects_fetched": "projects_fetched.csv",
        # Generated after selecting projects
        "projects": "projects.csv",
        # Generated in collect_data.py
        "directory": directory,
        "checkpoint": directory + f"{project}_checkpoint.db",
        "pulls_raw": directory + f"{project}_pulls.db",
        "timelines_raw": directory + f"{project}_timelines.db",
        "commits": directory + f"{project}_commits.db",
        "patches_raw": directory + f"{project}_patches.db",
        "metadata": directory + f"{project}.db",
        # Generated in preprocess_data.py
        "timelines_fixed": directory + f"{project}_timelines_fixed.db",
        "timelines": directory + f"{project}_timelines.csv",
        "pulls": directory + f"{project}_pulls.csv",
        "patches": directory + f"{project}_patches.csv",
        # Generated manually
        "bots": "bots.csv",
        # Generated in process_data.py
        "dataset": directory + f"{project}_dataset.csv",
        # Generated in postprocess_data.py
        "statistics": "statistics.csv",
        # Generated in measure_features_maintainers.py
        "features_maintainers": directory + f"{project}_features_maintainers.csv",
        # Generated in measure_features_contributors.py
        "features_contributors": directory + f"{project}_features_contributors.csv",
    }
    return pathlib.Path(files[file])


def force_refresh():
    parser = argparse.ArgumentParser()
    parser.add_argument("-y", action="store_true", help="force fresh start")
    parser.add_argument("-n", action="store_true", help="do not force fresh start")
    if (args := parser.parse_args()).y:
        return True
    elif args.n:
        return False


def cleanup_files(files, fresh=None, project=None):
    if not isinstance(files, list):
        files = [files]
    files = [get_path(file, project) for file in files]
    if (exist := any([file.exists() for file in files])) and fresh is None:
        message = "Do you want to force fresh start? [y/n] "
        if project is not None:
            message = f"{project}: {message}"
        while True:
            if (fresh := input(message).lower()) in ["y", "n"]:
                fresh = True if fresh == "y" else False
                break
    if fresh:
        for file in files:
            file.unlink(missing_ok=True)
    return True if fresh or not exist else False


def check_files(files, project, exclude=None):
    if not isinstance(files, list):
        files = [files]
    if exclude is None:
        exclude = []
    elif not isinstance(exclude, list):
        exclude = [exclude]
    return all([get_path(file, project).exists() for file in files]) and not any(
        [get_path(file, project).exists() for file in exclude]
    )


def open_database(file):
    def encode(data):
        return json.dumps(data, ensure_ascii=False, separators=(",", ":"))

    def decode(data):
        return json.loads(data)

    return sqlitedict.SqliteDict(file, tablename="data", autocommit=True, encode=encode, decode=decode)


def convert_dtypes(function):
    def wrapper(*args, **kwargs):
        dataframe = function(*args, **kwargs)
        for column in dataframe.filter(regex="^time|_at$"):
            dataframe[column] = pd.to_datetime(dataframe[column]).dt.tz_localize(None)
        dataframe = dataframe.convert_dtypes()
        for column in dataframe.select_dtypes("string"):
            if dataframe[column].nunique() < dataframe[column].count():
                dataframe[column] = dataframe[column].astype("category")
        return dataframe

    return wrapper


def import_events(file):
    return pd.read_json(file, lines=True)


@convert_dtypes
def import_projects_fetched():
    return pd.read_csv(get_path("projects_fetched"), index_col="project", low_memory=False)


@convert_dtypes
def import_projects():
    return pd.read_csv(get_path("projects"), index_col="project", low_memory=False)


def open_checkpoint(project):
    return open_database(get_path("checkpoint", project))


def open_pulls_raw(project):
    return open_database(get_path("pulls_raw", project))


def open_timelines_raw(project):
    return open_database(get_path("timelines_raw", project))


def open_commits(project):
    return open_database(get_path("commits", project))


def open_patches_raw(project):
    return open_database(get_path("patches_raw", project))


def open_metadata(project):
    return open_database(get_path("metadata", project))


def open_timelines_fixed(project):
    return open_database(get_path("timelines_fixed", project))


@convert_dtypes
def import_timelines(project):
    return pd.read_csv(get_path("timelines", project), index_col=["pull_number", "event_number"], low_memory=False)


@convert_dtypes
def import_pulls(project):
    return pd.read_csv(
        get_path("pulls", project), index_col="number", quoting=csv.QUOTE_ALL, escapechar="\\", low_memory=False
    )


@convert_dtypes
def import_patches(project):
    return pd.read_csv(get_path("patches", project), index_col=["pull_number", "sha"], low_memory=False)


@convert_dtypes
def import_bots():
    return pd.read_csv(get_path("bots"), index_col="bot", low_memory=False)


@convert_dtypes
def import_dataset(project):
    return pd.read_csv(get_path("dataset", project), index_col=["pull_number", "event_number"], low_memory=False)


@convert_dtypes
def import_statistics():
    return pd.read_csv(get_path("statistics"), index_col="project", low_memory=False)


@convert_dtypes
def import_features_maintainers(project):
    return pd.read_csv(get_path("features_maintainers", project), index_col=["pull_number"], low_memory=False)


@convert_dtypes
def import_features_contributors(project):
    return pd.read_csv(get_path("features_contributors", project), index_col=["pull_number"], low_memory=False)


def tocollect():
    return import_projects_fetched().index


def selected():
    return import_projects().index


def collected():
    return [
        project
        for project in tocollect()
        if check_files(
            ["pulls_raw", "timelines_raw", "commits", "patches_raw", "metadata"], project, exclude="checkpoint"
        )
    ]


def toanalyze():
    return selected().intersection(collected())


def preprocessed():
    return [
        project
        for project in toanalyze()
        if check_files(["timelines", "pulls", "patches"], project, exclude="timelines_fixed")
    ]


def processed():
    return [project for project in preprocessed() if check_files("dataset", project)]


def measured_maintainers():
    return [project for project in processed() if check_files("features_maintainers", project)]


def measured_contributors():
    return [project for project in processed() if check_files("features_contributors", project)]
