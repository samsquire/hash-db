#!/bin/bash

sudo -E $(which gunicorn) -w 1 -k gevent  server:app --bind 127.0.0.1:1005 &
sleep 1

export HASHDB_ARGS="--server localhost:1005 --port 1006" ; sudo -E $(which gunicorn) -w 1 -k gevent  client:app --bind 127.0.0.1:1006 &
sleep 1
export HASHDB_ARGS="--server localhost:1005 --port 1007" ; sudo -E $(which gunicorn) -w 1 -k gevent  client:app --bind 127.0.0.1:1007 &
sleep 1

for job in `jobs -p`
do
echo $job
    wait $job
done