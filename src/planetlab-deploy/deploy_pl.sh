#!/bin/bash

while read host; do
    echo "==== $host"
    ssh -n -i ~/.ssh/id_rsa rice_comp529@${host} "rm -f * && wget http://avs4.web.rice.edu/awsping/setup_pl.sh && chmod 777 setup_pl.sh && ./setup_pl.sh"
done < planetlab.txt

