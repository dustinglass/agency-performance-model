#!/bin/bash

source ~/.virtualenvs/agency-performance-model/bin/activate
cd ~/sites/agency-performance-model
kaggle datasets download -d moneystore/agencyperformance
python etl.py