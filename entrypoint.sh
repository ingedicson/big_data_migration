#!/bin/sh

if [ "$1" = 'load_data' ]; then
  exec python load_data.py
elif [ "$1" = 'app' ]; then
  exec python app.py
else
  echo "Usage: $0 {load_data|app}"
  exit 1
fi
