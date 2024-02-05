#!/bin/bash

cd "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)" &&
    python3 fetch_projects.py -n &&
    python3 collect_data.py -n &&
    echo "Finished downloading data"
