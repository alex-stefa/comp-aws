#!/bin/bash

./awscommand.py STATUS 168.7.23.167 8080

tail -n 120 log_server.log
