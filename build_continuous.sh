#!/bin/bash

clear

while true; do
  ./build.sh $1

  change=$(inotifywait -r -e close_write,moved_to,create,modify . \
    --exclude 'src/main$|bin/.*|.*\.log|nimcache|.changes|#.*' \
    2> /dev/null)

  # very short sleep to avoid "text file busy"
  sleep 0.01

  clear
  echo "changed [`date +%T`]: $change"
  echo "changed [`date +%T`]: $change" >> .changes
done
