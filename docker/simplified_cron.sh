#!/bin/bash

# Switch to local timezone
ln -snf /usr/share/zoneinfo/$TZ /etc/localtime

# Create cron tasks & logfile
cp /ls_build/services/simplified_crontab /etc/cron.d/metadata
touch /var/log/cron.log
