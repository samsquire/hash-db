#!/bin/bash

sudo -E $(which gunicorn) -w 1 -k gevent  server:app --bind 127.0.0.1:1005 &

export HASHDB_ARGS="--server localhost:1005 --port 1006" ; sudo -E $(which gunicorn) -w 1 -k gevent  client:app --bind 127.0.0.1:1006 &
export HASHDB_ARGS="--server localhost:1005 --port 1007" ; sudo -E $(which gunicorn) -w 1 -k gevent  client:app --bind 127.0.0.1:1007 &

for job in `jobs -p`
do
echo $job
    wait $job
done
