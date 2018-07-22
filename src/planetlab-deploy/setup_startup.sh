#!/bin/bash

cd /home/rice_comp529

rm -f init_pl.conf

wget http://avs4.web.rice.edu/awsping/init_pl.conf

cp ./init_pl.conf /etc/init/init_pl.conf

chmod u+x /etc/init/init_pl.conf

