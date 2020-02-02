#!/bin/bash
# simple script to refresh changes to Flask app and exit out of EC2 instance
sudo apachectl restart
screen -S <screen_name>

#  ctrl+a+d
