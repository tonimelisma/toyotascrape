#!/bin/bash

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

$SCRIPT_DIR/.venv/bin/python3 $SCRIPT_DIR/scrape.py &&
$SCRIPT_DIR/.venv/bin/python3 $SCRIPT_DIR/transform.py &&
$SCRIPT_DIR/.venv/bin/python3 $SCRIPT_DIR/calc_dealer_markups.py &&
$SCRIPT_DIR/.venv/bin/python3 $SCRIPT_DIR/calc_new_cars_wo_markups.py