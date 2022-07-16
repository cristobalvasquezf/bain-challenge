#!/bin/bash

while getopts ":v:" opt; do
    echo ${getopts}
    case ${opt} in
        v)
            MODEL_VERSION=${OPTARG}
            ;;
        \?)
            echo "Usage: cmd "
            exit 1
            ;;
        :)
            echo "Invalid option: $OPTARG requires an argument" 1>&2
            exit 1
            ;;
    esac
done
shift $((OPTIND -1))

docker build --build-arg MODEL_VERSION=$MODEL_VERSION -t ml-milk-app:latest .
docker run -d --name milk-api -p 80:80 ml-milk-app:latest