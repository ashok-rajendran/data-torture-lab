#!/bin/bash

# File to store the log
LOGFILE="/tmp/system_health.log"

echo "===== System Health Report =====" > $LOGFILE
echo "Date: $(date)" >> $LOGFILE
echo "" >> $LOGFILE

# Uptime
echo "Uptime:" >> $LOGFILE
uptime >> $LOGFILE
echo "" >> $LOGFILE

# CPU and Memory Usage
echo "CPU & Memory Usage:" >> $LOGFILE
top -bn1 | head -n 5 >> $LOGFILE
echo "" >> $LOGFILE

# Disk Usage
echo "Disk Usage:" >> $LOGFILE
df -h >> $LOGFILE
echo "" >> $LOGFILE

# Internet Connection Test
echo "Internet Connection:" >> $LOGFILE
ping -c 2 google.com &> /dev/null && echo "Online" >> $LOGFILE || echo "Offline" >> $LOGFILE
echo "" >> $LOGFILE

echo "Report saved to $LOGFILE"
