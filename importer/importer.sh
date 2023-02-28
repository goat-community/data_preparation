#!/usr/bin/bash
# Exit if any command fails
set -e
batch_size=10

city=$1

# If city is not downloaded before, download it.
if test -f "temp/$city.zip"; then
    echo "city was downloaded before. going for the next step."
else
    if [ ! -d "temp" ]; then
        mkdir temp
    else
        rm -r temp/*
    fi
    aws s3 cp "s3://goat30-data/300001907_LoD2/lod2/$city/$city.zip" temp/
    echo "downloaded city $city"
fi

# If city is not unzipped before, unzip it.
if [ ! -d "temp/$city" ]; then
    unzip "temp/$city.zip" -d "temp/$city"
fi
mkdir "temp/$city/done/"

# While there are files to import
while compgen -G "temp/$city/*.xml" > /dev/null; do
    # Get docker down
    docker compose down --volumes

    # Get docker up
    docker compose up -d --wait

    # Wait for docker to get ready
    sleep_time=10
    while [ "$sleep_time" -gt 0 ];
    do
        echo -en "\rWait $sleep_time seconds to ensure 3d city is ready..."
        sleep_time=$(($sleep_time-1))
        sleep 1
    done
    echo ''
    
    # Resize batch size if there are not enough files
    all_files=(temp/$city/*.xml)
    files_count=${#all_files[@]}
    if [ "$files_count" -lt "$batch_size" ]; then
        batch_size=$files_count
    fi

    # Import data
    for file_path in "${all_files[@]: -$batch_size}"; do
        echo "importing $file_path"
        impexp import -c settings.xml $file_path
        mv "$file_path" "./temp/$city/done/"
    done
done