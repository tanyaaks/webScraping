#!/bin/bash

echo "Starting gathering bycard data"
python3.9 send_to_kafka.py --src bycard &
sleep 3  &
echo "Starting gathering ticketpro data"
python3.9 send_to_kafka.py --src ticketpro