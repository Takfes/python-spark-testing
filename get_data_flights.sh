#!/bin/bash

if [ ! -d "flights" ]; then
    mkdir flights
fi

cd flights

if [ ! -f "flight-delays.zip" ]; then
    kaggle datasets download -d usdot/flight-delays
fi

unzip flight-delays.zip
