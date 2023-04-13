#!/bin/bash

if [ ! -d "movies" ]; then
    mkdir movies
fi

cd movies

if [ ! -f "the-movies-dataset.zip" ]; then
    kaggle datasets download -d rounakbanik/the-movies-dataset
fi

unzip the-movies-dataset.zip
