#!/bin/sh

TODAY=`date +%Y-%m-%d`
YESTERDAY=`date +%Y-%m-%d -d "yesterday"`

echo "Starting to run pull-transactions"
python pull-transactions.py $YESTERDAY $TODAY

echo "Completed pull-transactions"