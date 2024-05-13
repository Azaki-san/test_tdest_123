#!/bin/bash

# Check if the config.conf file exists
if [ ! -f "config.conf" ]; then
    echo "Error: config.conf file not found!"
    exit 1
fi

# Read each line from the config.conf file
while IFS=' ' read -r id address
do
    # Start a new GNOME terminal for each node and run the python script with the node ID
    osascript -e "tell application \"Terminal\" to do script \"python3 /Users/andrey/Innopolis/DNP/week6/src/node.py $id\"" &
done < config.conf

echo "All nodes have been started in separate terminals."
