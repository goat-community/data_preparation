#!/usr/bin/bash
bridge='bridge'
while true
do
    stat=`cat $bridge/stat.txt`
    # Check if we are ready to import new files.
    if [ "$stat" == 'ready' ]; then
        echo 'Its ready.'
        city=`cat $bridge/city_name.txt`

        # If city is not downloaded before, download it.
        if test -f "temp/$city.zip"; then
            echo "city was downloaded before. going for the next step."
        else
            rm -r temp/*
            aws s3 cp "s3://goat30-data/300001907_LoD2/lod2/$city/$city.zip" temp/
            echo "downloaded city $city"
        fi
        
        # Remove all zip files except the city we are working on.
        # ls temp/*.zip | grep -xv "$city.zip" | parallel rm
        # If city not unzipped, unzip it.
        if [ ! -d "temp/$city" ]; then
            unzip "temp/$city.zip" -d "temp/$city"
        fi

        # Read file names and edit to fit to temp directory
        readarray -t file_names_ < "$bridge/list_of_files.txt"
        file_names=()
        for file_name in $file_names_
        do
            file_names+=("temp/$city/$file_name")
        done

        # Create zip for import
        if test -f "temp/to_import.zip"; then
            rm temp/to_import.zip
        fi
        zip -j temp/to_import ${file_names[@]}

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
        
        # import data
        impexp import -c settings.xml temp/to_import.zip

        echo 'imported' > "$bridge/stat.txt"
        echo ''
    fi
    echo -en '\rWaiting for data to come in.'
    sleep 4
done