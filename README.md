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

The exact version and the warning can be ignored

## Starting a jupyter notebook server

Log into one of the lyoui* machines.

We're going to start a jupyter notebook server on one of the workers of the IP2I Slurm Farm.

First, choose your favourite number between 8000 and 10000, say it's XXXX.

Start your server on one of the workers: 

```
srun jupyter notebook --no-browser --port=XXXX --ip=0.0.0.0
```

We specified: 

* the port on which the server can be reached on the worker
* that the server can be reached from any ip addresss (`--ip` option)

Now check on which worker your server is running from another terminal

```
squeue -u $USER
```

```
542960    normal  jupyter  cbernet  R       4:53      1 lyoworkYZW
```

## Running a jupyter notebook on your server

**If you're in the lab:**

Point your browser to `http://lyoworkYZW:XXXX` (change the name of the worker and the port appropriately), and enter your token, which can be found in the logs of your jupyter notebook command.

Open the first notebook below. 

**If you're outside the lab:**

The worker is not reachable from outside the lab. So we're going to do ssh tunnelling. 

From your machine at home, do: 

```
 ssh -L 8888:lyoworkXYZ:XXXX -L 8787:lyoworkXYZ:8787 lyoserv 
```

This is going to connect you to lyoserv from home, and will map: 

* port 8888 on your machine to the port of your jupyter notebook server on your worker
* port 8787 on your machine to port 8787 on your worker. As we will see later, this port will be used for the dask dashboard

On your machine at home, point your browser to http://localhost:8888

## Tutorials

* [dask_slurm.ipynb](dask_slurm.ipynb): How to run dask on the IP2I Slurm farm (dask delayed)
* [dask_dataframe_slurm.ipynb](dask_dataframe_slurm.ipynb): Analysis of a large text dataset (dask dataframe)
