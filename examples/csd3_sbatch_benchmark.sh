#!/bin/bash

#! Slurm options ###############################
#SBATCH -J ska_sdp_spectral_line_imaging
#SBATCH -p icelake
#SBATCH --nodes=3
#SBATCH --ntasks=4
#SBATCH --time=02:00:00
#SBATCH --mem=64000

#SBATCH --no-requeue

numnodes=$SLURM_JOB_NUM_NODES
numtasks=$SLURM_NTASKS
mpi_tasks_per_node=$(echo "$SLURM_TASKS_PER_NODE" | sed -e  's/^\([0-9][0-9]*\).*$/\1/')

SETUP=". /etc/profile.d/modules.sh ;  \
    module purge;                     \
    module load rhel8/default-icl;    \
    module load openmpi-4.0.5-gcc-8.4.1-l7ihwk3; \
    module load miniconda3/4.5.1; \
    source activate spec_line"

eval $SETUP

## Custom Env vars

export DUCC0_NUM_THREADS=${SLURM_CPUS_PER_TASK}

WORKDIR=/rds/user/hpcnima1/hpc-work/

INPUT_MS=$WORKDIR/ska_low_sim.ps/
INPUT_CONFIG=$WORKDIR/spectral_line_imaging_ska_low_sim.yml
DASK_WORKERS_PER_NODE=4
DASK_PORT=8786
DASK_LOGS_DIR=$WORKDIR/logs

workdir=$WORKDIR


#! Are you using OpenMP (NB this is unrelated to OpenMPI)? If so increase this
#! safe value to no more than 76:
export OMP_NUM_THREADS=1

#! Number of MPI tasks to be started by the application per node and in total (do not change):
np=$[${numnodes}*${mpi_tasks_per_node}]

export I_MPI_PIN_DOMAIN=omp:compact # Domains are $OMP_NUM_THREADS cores in size
export I_MPI_PIN_ORDER=scatter # Adjacent domains have minimal sharing of caches/sockets

NODES=($(scontrol show hostnames))
HEAD_NODE="$(hostname)"

# Join array into space-separated string
NODES_SPACE_SEPARATED="${NODES[*]}"

echo "Allocated nodes: $NODES_SPACE_SEPARATED"
echo "Head node: $HEAD_NODE"

##### Start dask scheduler on head node
DASK_SCHEDULER_ADDR=$HEAD_NODE:$DASK_PORT

dask scheduler --port ${DASK_PORT} >$DASK_LOGS_DIR/scheduler_$HEAD_NODE.log 2>&1 &
echo "Started dask scheduler on $DASK_SCHEDULER_ADDR"

dask_worker_command="dask worker $DASK_SCHEDULER_ADDR --name $node \
        --nworkers $DASK_WORKERS_PER_NODE --resources subprocess_slots=1"

for node in "${NODES[@]}"; do
    logfile=$DASK_LOGS_DIR/worker_$node.log
    ssh $node "$SETUP ; piper benchmark --command '${dask_worker_command}' --output-file-prefix=node_${node}\
        >$logfile" 2>&1 &
    echo "Started benchmarked dask worker on $node"
done

dask_options="--dask-scheduler $DASK_SCHEDULER_ADDR"
command="spectral-line-imaging-pipeline run --input ${INPUT_MS} --config ${INPUT_CONFIG} ${dask_options}"

#! Full path to application executable: 
application="piper"

#! Run options for the application:
options="benchmark --command '${command}' --setup"


CMD="$application $options"

cd $workdir
echo -e "Changed directory to `pwd`.\n"

JOBID=$SLURM_JOB_ID

echo -e "JobID: $JOBID\n======"
echo "Time: `date`"
echo "Running on master node: `hostname`"
echo "Current directory: `pwd`"

if [ "$SLURM_JOB_NODELIST" ]; then
        #! Create a machine file:
        export NODEFILE=`generate_pbs_nodefile`
        cat $NODEFILE | uniq > machine.file.$JOBID
        echo -e "\nNodes allocated:\n================"
        echo `cat machine.file.$JOBID | sed -e 's/\..*$//g'`
fi

echo -e "\nnumtasks=$numtasks, numnodes=$numnodes, mpi_tasks_per_node=$mpi_tasks_per_node (OMP_NUM_THREADS=$OMP_NUM_THREADS)"

echo -e "\nExecuting command:\n==================\n$CMD\n"

eval $CMD 
