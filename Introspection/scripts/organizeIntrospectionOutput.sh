#!/bin/bash

source introspection_header.sh

if [ -z "$1" ]; then
        echo "Itrospection Output Directory Not Provided."
        exit 1
else
	echo "Introspection Output Directory is - $1"
fi

for f in $(ls IntrospectionOutput* $1); do
	echo $f
	IFS='#' read -ra tokens <<< "$f"
        numTokens=${#tokens[@]}
        echo $numTokens
        if (($numTokens != 5)); then
                continue
        fi
	outdir=$1/${tokens[1]}/${tokens[2]}/${tokens[3]}
	echo $outdir
        mkdir -p $outdir
	cp $1/$f $outdir/${tokens[4]}
	rm $1/$f
	#echo ${tokens[0]}
        #echo ${tokens[1]}
	#echo ${tokens[2]}
	#echo ${tokens[3]}

done

