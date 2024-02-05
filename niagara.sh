#!/bin/bash

#SBATCH --time=0-24
#SBATCH --nodes=1
#SBATCH --cpus-per-task=80
#SBATCH --mail-type=ALL

echo "Started analyzing data"
module load NiaEnv/2022a python/3.11
source venv/bin/activate
export IPYTHONDIR=$SCRATCH/.ipython
export MPLCONFIGDIR=$SCRATCH/.matplotlib

python preprocess_data.py -n
python process_data.py -n
python postprocess_data.py -n
python measure_features_maintainers.py -n &
python measure_features_contributors.py -n &
wait
papermill analytics_maintainers.ipynb analytics_maintainers.ipynb &
papermill analytics_contributors.ipynb analytics_contributors.ipynb &
wait

deactivate
echo "Finished analyzing data"
