#!/bin/bash

echo "Input HDFS Directory is - $1"
echo "Merged Output goes to - $2"

if $(hadoop fs -test -d $1) ; then echo "";else echo "Input HDFS Directory does not exist...exiting"; exit; fi
if $(hadoop fs -test -d $2) ; then echo "Output directory exists already. Going to merge files"; else  "Output directory does not exist..creating"; hadoop fs -mkdir -p $2; fi

for f in $(hadoop fs -ls $1| sed '1d;s/  */ /g' | cut -d\  -f8 | xargs -n 1 basename); do
	if echo "$f" | grep -q ".json"; then
	 infile="$1/$f"
         if $(hadoop fs -test -d $infile); then echo "Going to copy $infile"; else echo "File $infile does not exist..Moving to next file"; continue; fi

         outfile="$2/$f"
         if $(hadoop fs -test -f $outfile); then echo "File $outfile already exists. Moving to next"; else hadoop fs -text $infile/part* | hadoop fs -put - $outfile; fi
	fi
done


