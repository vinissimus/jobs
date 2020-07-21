#!/bin/bash

case $1 in
    clean)
	isort -rc jobs/
	black jobs -l 80 
	;;
   *)
        echo -e "Usage: \n"
	echo -e "\t clean: Cleans code (black and isort)"
	;;
esac


