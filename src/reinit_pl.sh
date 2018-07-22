#!/bin/bash


while [ 1 -gt 0 ]; do
    
    date
    
    while read host; do
        echo "==== $host"
        ssh -n -i ~/.ssh/id_rsa rice_comp529@${host} "if [ -z \"\$(pgrep python)\" ]; then echo \`hostname\` dead; rm -f setup_pl.sh; wget http://avs4.web.rice.edu/awsping/setup_pl.sh; chmod 777 setup_pl.sh; ./setup_pl.sh; sh -c 'nohup python awsclient.py avs4.cs.rice.edu 8080 > client.out 2> client.err < /dev/null &'; echo restarted; else echo \`hostname\` alive; fi"
    done < planetlab.txt
    
    sleep 7000   
done

