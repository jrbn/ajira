#!/bin/bash

PATH_TO_BASEDIR="/home/mocke/MasterProj"
PATH_TO_ARCH=$PATH_TO_BASEDIR"/arch"
PATH_TO_QUERYPIE=$PATH_TO_BASEDIR"/querypie-storage"

# clean querypie-storage log
echo "CLEAN query-pie log"
rm -rf $PATH_TO_QUERYPIE"/log/"*

