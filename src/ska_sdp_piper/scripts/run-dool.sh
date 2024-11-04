#! /usr/bin/env bash

output_dir=$1
mkdir -p $output_dir
file_prefix="${2}"
timestamp=`date +%Y-%m-%dT%T`
file_path="${output_dir}/${file_prefix}_${timestamp}.csv"

DELAY_IN_SECONDS=${DELAY_IN_SECONDS:-5}

echo "Starting Dool ..."
echo "Benchmark report is written to ${file_path}"

$DOOL_BIN --time --mem --swap --io --aio \
          --disk --fs --net --cpu --cpu-use \
          --output ${file_path} $DELAY_IN_SECONDS &
DOOL_PID=$!

echo -e "Running benchmark on: \n ${@:3} \n"

sleep 2
${@:3}
sleep 2

kill -9 $DOOL_PID
