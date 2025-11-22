#!/bin/bash

if [[ -z "$ZOOBINDIR" ]]
then
	echo "Error!! ZOOBINDIR is not set" 1>&2
	exit 1
fi

. $ZOOBINDIR/zkEnv.sh

#TODO Include your ZooKeeper connection string here. Make sure there are no spaces.
# 	Replace with your server names and client ports.
export ZKSERVER=tr-open-16.cs.mcgill.ca:21811,tr-open-18.cs.mcgill.ca:21811,tr-open-19.cs.mcgill.ca:21811
# export ZKSERVER=open-gpu-XX.cs.mcgill.ca:218XX,open-gpu-XX.cs.mcgill.ca:218XX,open-gpu-XX.cs.mcgill.ca:218XX

java -cp $CLASSPATH:../task:.: DistProcess 
