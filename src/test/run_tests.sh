#!/bin/bash

if [ $# -ne 1 ]
then
  echo "Usage: `basename $0` <number of test runs>"
  exit 1
fi

num_tests_runs=$1

function run_tests {
  log_file="tests-$(date +%d-%m-%Y_%H-%M-%S).log"
  echo "Using log file $log_file"
  sbt test 2>&1 | tee -a $log_file
  sleep 1
}

# --------------
# Main
# --------------
echo "Running tests $num_tests_runs times"
echo 
for test_run_id in $(seq 1 $num_tests_runs)
do 
  echo '-------------------------------'
  echo "Test run $test_run_id"
  run_tests
  echo '-------------------------------'
done


  
