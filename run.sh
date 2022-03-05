
# Check if docker exists
if [ ! -x "$(command -v docker)" ]; then
    echo "Install docker"
    exit 0
fi

# Check if docker compose exists
if [ ! -x "$(command -v docker-compose)" ]; then
    echo "Install docker"
    exit 0
fi

# Check if python exists
if [ ! -x "$(command -v python3)" ]; then
    echo "Install python3"
    exit 0
fi

# Check if pip exists
if [ ! -x "$(command -v pip)" ]; then
    echo "Install pip"
    exit 0
fi

echo -ne "Installing requirements...\n"
pip install -r requirements.txt && \
echo -ne "  Done!\n\n"

echo -ne "-----------------------------------\n\n"

echo -ne "Initializing postgres container...\n"
docker-compose up -d && \

for i in {1..50}
do
    sleep 5s
    if [ "$(docker ps -aq -f status=running -f name=postgres)" ]; then
        echo -ne " Done!\n"
        python3 main.py && \
        break
    fi
done


echo -ne "Press any key to continue...\n"
read -n 1 -s

exit 0