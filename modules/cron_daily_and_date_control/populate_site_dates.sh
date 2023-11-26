#!/bin/bash

# If global END_DATE unset, set to 2 days ago
echo "Global start date = $START_DATE"
if [ -z $END_DATE ]; then 
  END_DATE=$(date -u -d "2 days ago" +%Y-%m-%d);
  echo "Input END_DATE is unset. Using 2 days previous."
fi
echo "Global end date = $END_DATE"

# Open the site list file to extract the sites and their corresponding site start/end and kafka start dates (if available)
jq -c '.[]' $SITE_FILE | while read i; do 
  site=$(echo $i | jq -r .site);
  echo "$site";
  site_start_date=$(echo $i | jq -r .start_date);
  echo "Site start date= $site_start_date";
  kafka_site_start_date=$(echo $i | jq -r .kafka_start_date);
  echo "Kafka site start date= $kafka_site_start_date";
  site_end_date=$(echo $i | jq -r .end_date);
  echo "Site end date= $site_end_date";

  unset start_date_trino
  unset end_date_trino
  unset start_date_kafka
  unset end_date_kafka
  
  
  # Preliminary logic for start and end dates
  if [ -z $START_DATE ] && [ $site_start_date = "null" ] && [ $kafka_site_start_date = "null" ]; then
    # Nothing to do
    echo "No global START_DATE and no site or kafka start dates indicated for $site. Skipping...";
    continue;
  fi
  
  # Make sure site start date is never null
  if [ -z $START_DATE ] && [ $site_start_date = "null" ]; then
    site_start_date=$kafka_site_start_date;
  elif [ $site_start_date = "null" ]; then
    site_start_date=$START_DATE;
    echo "Setting site start date to global start date $START_DATE"
  fi
  # Make sure site start date is on or after global start date
  if ! [ -z $START_DATE ] && [ $(date -u +%s -d $site_start_date) -lt $(date -u +%s -d $START_DATE) ]; then
    site_start_date=$START_DATE;
  fi
  # Make sure kafka site start date is on or after global start date
  if ! [ -z $START_DATE ] && ! [ $kafka_site_start_date = "null" ] && [ $(date -u +%s -d $kafka_site_start_date) -lt $(date -u +%s -d $START_DATE) ]; then
    kafka_site_start_date=$START_DATE;
  fi
  
 
  # Make sure site end date is never null
  if [ $site_end_date = "null" ]; then
    site_end_date=$END_DATE;
  fi
  # Make sure site end date is on or before global end date
  if [ $(date -u +%s -d $site_end_date) -gt $(date -u +%s -d $END_DATE) ]; then
    site_end_date=$END_DATE;
  fi
  
  # Now we can ignore global start and end dates, paying attention only to site start/end and kafka start dates
  
  # Make sure site end date is on or after site start date
  if [ $(date -u +%s -d $site_end_date) -lt $(date -u +%s -d $site_start_date) ]; then
    # Nothing to do
    echo "Start date $site_start_date is after end date $site_end_date for $site. Skipping...";
    continue;
  fi
  # Make sure kafka start date is on or after site start date 
  if ! [ $kafka_site_start_date = "null" ] && [ $(date -u +%s -d $kafka_site_start_date) -lt $(date -u +%s -d $site_start_date) ]; then
    echo "Adjusting Kafka start date to $site_start_date";
    kafka_site_start_date=$site_start_date;
  fi

    
  # Determine dates for kafka data vs. trino data
  if [ $kafka_site_start_date = "null" ] || [ $(date -u +%s -d $kafka_site_start_date) -gt $(date -u +%s -d $site_end_date) ]; then 
    # Trino only
    start_date_trino=$site_start_date;
    end_date_trino=$site_end_date;
  elif [ $(date -u +%s -d $kafka_site_start_date) = $(date -u +%s -d $site_start_date) ]; then
    # Kafka only
    start_date_kafka=$kafka_site_start_date;
    end_date_kafka=$site_end_date;
  else 
    start_date_trino=$site_start_date;
    end_date_trino=$(date -u +%Y-%m-%d -d "$kafka_site_start_date -1 day");
    start_date_kafka=$kafka_site_start_date;
    end_date_kafka=$site_end_date;
  fi
  echo "Import triggers for $site: Trino start: $start_date_trino; Trino end: $end_date_trino; Kafka start: $start_date_kafka; Kafka end: $end_date_kafka"
  
  
  # Create daily folder structure for trino data
  echo "Creating import trigger folder structure for $site"
  if ! [ -z $start_date_trino ]; then
    for d in $(seq $(date -u +%s -d $start_date_trino) +86400 $(date -u +%s -d $end_date_trino)) ; do
      date_path=$(date -u +%Y/%m/%d -d @$d);
      mkdir -p $OUT_PATH/$SOURCE_TYPE/$date_path;
      echo $site > $OUT_PATH/$SOURCE_TYPE/$date_path/$site;
    done;
  fi
  
  # Create daily folder structure for kafka data
  if ! [ -z $start_date_kafka ]; then
    for d in $(seq $(date -u +%s -d $start_date_kafka) +86400 $(date -u +%s -d $end_date_kafka)) ; do
    date_path=$(date -u +%Y/%m/%d -d @$d);
    mkdir -p $OUT_PATH/$SOURCE_TYPE/$date_path;
    echo $site.kafka > $OUT_PATH/$SOURCE_TYPE/$date_path/$site.kafka;
    done;
  fi
  done
  
  # Finally, create a file listing the data years (for the metadata assignment pipelines)
  years=$(ls $OUT_PATH/$SOURCE_TYPE)
  for year in $years; do
    echo $year > $OUT_PATH/data_years_$year.txt
  done