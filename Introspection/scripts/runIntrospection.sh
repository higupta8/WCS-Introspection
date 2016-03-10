#!/bin/bash

source introspection_header.sh

echo "SPARK_HOME is $SPARK_HOME"
echo "BI_HOME is $BI_HOME"
echo "SPARK_MASTER is $SPARK_MASTER"
echo "Input Directory is - $1"
echo "Introspection Output goes to - $2"


mkdir -p $2
echo "Introspection Input Parameters are specified in file - $3"

if [ -z "$1" ]; then
        echo "Input Directory Not Set."
        exit 1
fi

if [ -z "$2" ]; then
        echo "Introspection Output Directory Not Set"
        exit 1
fi

if [ -z "$3" ]; then
        echo "Introspection Input Parameter File Not Set"
        exit 1
fi

while read line
do
        echo $line
        IFS=' ' read -ra tokens <<< "$line"
        numTokens=${#tokens[@]}
        echo $numTokens
        if (($numTokens < 2)); then
                echo "Less than 2 paameters in $line"
                echo "skipping processing this input"
                continue
        fi
        echo ${tokens[0]}
        echo ${tokens[1]}
        $SPARK_HOME/bin/spark-submit --class  ${tokens[0]} --jars $BI_HOME/lib/ibm-compression.jar $MONITORING_JAR $1  ${tokens[1]} $2 $SPARK_MASTER
done < $3

 ./organizeIntrospectionOutput.sh $2
