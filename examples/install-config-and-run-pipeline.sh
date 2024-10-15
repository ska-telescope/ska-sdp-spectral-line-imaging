#!/bin/bash

set -e

print_usage() {
  echo "Usage: $0 [options]"
  echo "Example $0 -i <msname> -d <dask-scheduler-ip> -o <output-dir> -- \ "
  echo "                            -s \"parameters.read_model.pols '[XX,YY]'\" \ "
  echo "                            -s 'parameters.read_model.image_name /path/to/image' \ "
  echo "                            -s <option 3> \ "
  echo "                            -s <option 4> \ "
  echo "Options: "
  echo "-i <msname>             Path to the measurement set"
  echo "-o <output-dir>         Output folder for data-products and logs"
  echo "-d <dask-scheduler-ip>  IP of dask scheduler"
  echo "-s \"key val\"          Key value to override in config"
}

if [ $# -eq 0 ]; then
  print_usage
  exit 0
fi

msname=""
daskip=""
outputdir=""
configdir=""
setargs=()

create_config() {
  spectral-line-imaging-pipeline install-config --config-install-path $configdir ${setargs[@]}
}

run_pipeline() {
  spectral-line-imaging-pipeline run --input $msname --output $outputdir \
    --dask-scheduler $daskip \
    --config $configdir/spectral_line_imaging_pipeline.yml
}

while getopts "i:o:c:d:s:h?" opt
do
  case "$opt" in
  [h?])
    print_usage
    exit 0
    ;;
  i)
    msname="$OPTARG"
    ;;
  c)
    configdir="$OPTARG"
    ;;
  o)
    outputdir="$OPTARG"
    ;;
  d)
    daskip="$OPTARG"
    ;;
  s)
    setargs+=("--set $OPTARG ")
    ;;
  *)
    print_usage
    exit 0
    ;;
  esac
done

shift $(($OPTIND - 1))

create_config
run_pipeline
