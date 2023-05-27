#!/bin/bash

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

$SCRIPT_DIR/.venv/bin/python3 $SCRIPT_DIR/scrape_cars.py &&
$SCRIPT_DIR/.venv/bin/python3 $SCRIPT_DIR/transform_cars.py &&
$SCRIPT_DIR/.venv/bin/python3 $SCRIPT_DIR/scrape_dealers.py &&
$SCRIPT_DIR/.venv/bin/python3 $SCRIPT_DIR/transform_dealers.py &&
$SCRIPT_DIR/.venv/bin/python3 $SCRIPT_DIR/join_cars_and_dealers.py &&
$SCRIPT_DIR/.venv/bin/python3 $SCRIPT_DIR/calc_dealer_markups.py &&
$SCRIPT_DIR/.venv/bin/python3 $SCRIPT_DIR/calc_new_cars_from_low-markup_dealers.py &&
$SCRIPT_DIR/.venv/bin/python3 $SCRIPT_DIR/calc_new_cars_wo_markups.py