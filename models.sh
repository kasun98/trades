#!/bin/bash

echo "Initializing ML model serving..."

cd src/forecast
waitress-serve --threads=4 --host=0.0.0.0 --port=5050 app:app
