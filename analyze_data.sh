#!/bin/bash

cd "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)" &&
    python3 preprocess_data.py -n &&
    python3 process_data.py -n &&
    python3 postprocess_data.py -n &&
    python3 measure_features_maintainers.py -n &&
    python3 measure_features_contributors.py -n &&
    echo "Finished analyzing data"
