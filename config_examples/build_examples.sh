#! /bin/bash

for file in `ls *.yaml`; do

	if [[ $file == "example_config.yaml" ]]; then
		continue
	fi

	echo Making example-$file

	cat example_config.yaml > example-$file
	cat $file >> example-$file
done
