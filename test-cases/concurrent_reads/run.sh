#!/usr/bin/env bash

BASE_DIR=test-cases/concurrent_reads
LOGS_DIR=$BASE_DIR/logs

python3 -u $BASE_DIR/read.py | tee > $LOGS_DIR/0.in | time ./client 0 config/vm.config > $LOGS_DIR/0.out &
python3 -u $BASE_DIR/read.py | tee > $LOGS_DIR/1.in | time ./client 1 config/vm.config > $LOGS_DIR/1.out &
python3 -u $BASE_DIR/read.py | tee > $LOGS_DIR/2.in | time ./client 2 config/vm.config > $LOGS_DIR/2.out &
python3 -u $BASE_DIR/read.py | tee > $LOGS_DIR/3.in | time ./client 3 config/vm.config > $LOGS_DIR/3.out &
python3 -u $BASE_DIR/read.py | tee > $LOGS_DIR/4.in | time ./client 4 config/vm.config > $LOGS_DIR/4.out &
python3 -u $BASE_DIR/read.py | tee > $LOGS_DIR/5.in | time ./client 5 config/vm.config > $LOGS_DIR/5.out &
python3 -u $BASE_DIR/read.py | tee > $LOGS_DIR/6.in | time ./client 6 config/vm.config > $LOGS_DIR/6.out &
python3 -u $BASE_DIR/read.py | tee > $LOGS_DIR/7.in | time ./client 7 config/vm.config > $LOGS_DIR/7.out &
python3 -u $BASE_DIR/read.py | tee > $LOGS_DIR/8.in | time ./client 8 config/vm.config > $LOGS_DIR/8.out &
python3 -u $BASE_DIR/read.py | tee > $LOGS_DIR/9.in | time ./client 9 config/vm.config > $LOGS_DIR/9.out &

wait