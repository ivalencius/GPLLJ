"""
Various constants that are useful for all files
"""
import os
import yaml

# Quality of life imports
from rich import print

### Change these paths for your own use
CWD = '/home/valencig/GPLLJ/' # change this
SCRATCH_DIR = '/scratch/valencig/GPLLJ-Scratch/'
###

os.chdir(CWD)
DATA_DIR = os.path.join(CWD, 'data/')
FIG_DIR = os.path.join(CWD, 'figures/')