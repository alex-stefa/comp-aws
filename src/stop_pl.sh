#!/bin/bash


while read host; do
    echo "==== $host"
    ssh -n -i ~/.ssh/id_rsa rice_comp529@${host} "killall python; killall -s KILL python"
done < planetlab.txt

