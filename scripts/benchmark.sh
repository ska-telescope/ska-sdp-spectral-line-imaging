#! /usr/bin/env bash

set -e

function install_and_setup () {
  git clone https://github.com/scottchiefbaker/dool.git
  cd dool
  ./install.py
}

function run_benchmarks_local () {
  local output_dir=$1
  mkdir -p $output_dir
  local file_prefix="spectral_line_imaging_pipeline_benchmark_"
  local timestamp=`date +%Y-%m-%dT%T`
  local file_path="${output_dir}/${file_prefix}${timestamp}.csv"

  if [[ -e $HOME/bin/dool ]]
  then 
    DOOL_BIN=$HOME/bin/dool
  else
    echo "Dool not found. Try installing with --setup."
    return 1
  fi

  DELAY_IN_SECONDS=5

  echo "Starting Dool ..."
  echo "Benchmark report is written to ${file_path}"

  $DOOL_BIN --time --mem --swap --io --aio --disk --fs --net --cpu --cpu-use --output ${file_path} $DELAY_IN_SECONDS &
  DOOL_PID=$!

  sleep 2

  echo -e "Running spectral line imaging pipeline with: \nspectral-line-imaging-pipeline ${@:2} \n"
  spectral-line-imaging-pipeline "${@:2}"
  sleep 2

  kill -9 $DOOL_PID
}

function main () {

  if [[ $1 == "--setup" ]]
  then
    echo "Installing Dool..."
    install_and_setup
  fi

  if [[ $1 == "--local" ]]
  then
    echo "Running benchmarks..."
    run_benchmarks_local "${@:2}"

  fi
}

main $@
