# Description of all codes

## Case files
- `setup-case.sh`: setting up 4 CESM2 cases with all momentum terms
- For branched cases with thermodynamic terms refer to `commands-for-Q-case.txt` and `cp-branch-files.sh`
- `extended_variable_info.csv`: all possible CESM2 variables (Note: some may output null values as they are depreciated/inactive)
- `transfer-files.sh`: transfers files from my scratch to the bccg scratch, clips CESM2 files to the NH
- `postprocesCESM2.py`: merge all CESM2 files, convert to isobaric coordinates, adjust UTC to LT, add topography
- `run__`: for running files on slurm cluster 
- `interpolation.py`: barely modified code from geocat to interpolate from hybrid-sigma coordinates to isobaric coordinates

## For sharing
- `GPLLJ.ipynb`: base file with all analysis
