#!/bin/bash

cd ~

rm -f awsclient.py
rm -f start_pl.sh
rm -f kill_pl.sh
rm -f startup_add.sh
rm -f startup_remove.sh

wget http://avs4.web.rice.edu/awsping/awsclient.py
wget http://avs4.web.rice.edu/awsping/start_pl.sh
wget http://avs4.web.rice.edu/awsping/kill_pl.sh
wget http://avs4.web.rice.edu/awsping/startup_add.sh
wget http://avs4.web.rice.edu/awsping/startup_remove.sh

chmod 777 awsclient.py
chmod 777 start_pl.sh
chmod 777 kill_pl.sh
chmod 777 startup_add.sh
chmod 777 startup_remove.sh



