#!/bin/sh

echo "Waiting for Prefect API at http://prefect:4200/api..."
until curl -s http://prefect:4200/api > /dev/null; do
  sleep 2
done

echo "Prefect API is up! Starting flow..."
python3 prefect/flows/crypto_flow.py