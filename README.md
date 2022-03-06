# Dask on the IP2I Slurm Farm

[Dask](https://dask.org/) provides advanced parallelism for analytics, enabling performance at scale for the tools you love. 

In a nutshell: 

* you reserve a pool of "workers". Each worker is capable of performing tasks
* you specify what you want to do, and dask computes a computing graph of the tasks that need to be done to reach your final result
* dask schedules the tasks to the workers in your pool in parallel, in such a way as to reach the final result as soon as possible

![](https://raw.githubusercontent.com/dask/dask-org/main/images/grid_search_schedule.gif)

Dask can run on: 

* a single machine
* a cluster of machines 
* a cluster with GPU machines 

## Prerequisite: Dask on a single machine 

Go through the following sections of the [Dask Tutorial](https://tutorial.dask.org/00_overview.html) on your machine: 

* https://tutorial.dask.org/00_overview.html
* https://tutorial.dask.org/01_dask.delayed.html
* https://tutorial.dask.org/01x_lazy.html

We're now ready to use the resources of the IP2I SLURM Farm with Dask! 

## Installing dask for the IP2I SLURM Farm

You should work on one of the `lyoui*` machines.

Install miniconda for your account: 

```bash
wget https://repo.anaconda.com/miniconda/Miniconda3-py39_4.10.3-Linux-x86_64.sh
bash Miniconda3-py39_4.10.3-Linux-x86_64.sh
```

Answer all questions, make sure everything went well, and then log out and log in again

Create your conda environment for this exercise:

```bash
conda create -n dask 
```

Activate it: 

```bash
conda activate dask
```

Install the needed packages: 

```bash
conda install -c conda-forge dask-jobqueue dask jupyter graphviz python-graphviz
```

Make sure that everything worked well: 

```
python -c "import dask_jobqueue; print(dask_jobqueue.__version__)"
```

You should get: 

```
/home/cms/cbernet/miniconda3/envs/dask/lib/python3.9/site-packages/dask_jobqueue/core.py:20: FutureWarning: tmpfile is deprecated and will be removed in a future release. Please use dask.utils.tmpfile instead.
  from distributed.utils import tmpfile
0.7.3
```

The warning can be ignored

## Tutorials

* [dask_slurm.ipynb](dask_slurm.ipynb): How to run dask on the IP2I Slurm farm (dask delayed)
* [dask_dataframe_slurm.ipynb](dask_dataframe_slurm.ipynb): Analysis of a large text dataset (dask dataframe)
