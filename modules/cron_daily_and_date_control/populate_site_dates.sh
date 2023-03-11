#!/bin/bash

# If END_DATE unset, set to 2 days ago
if [ -z $END_DATE ] 
then 
  END_DATE=$(date -u -d "2 days ago" +%Y-%m-%d);
  echo "Input END_DATE is unset. Using 2 days previous."
fi
echo "End date = $END_DATE"

# Open the site list file to extract the sites and their corresponding kafka start dates (if available)
jq -c '.[]' $SITE_FILE | while read i; do 
  site=$(echo $i | jq -r .site)
  echo "$site"
  kafka_site_start_date=$(echo $i | jq -r .kafka_start_date)
  echo "Kafka site start date= $kafka_site_start_date";
  
  # If START_DATE unset, no trino conversion. Only doing kafka loading
  if [ -z $START_DATE ] && [ $kafka_site_start_date = "null" ]
  then
    echo "No START_DATE set and no kafka start date indicated for $site. Skipping...";
    continue;
  elif [ -z $START_DATE ]
  then
    unset start_date_trino;
    unset end_date_trino;
    start_date_kafka=$kafka_site_start_date;
    end_date_kafka=$END_DATE;
    echo "No START_DATE set. Setting kafka data only from $start_date_kafka to $end_date_kafka (inclusive) for $site.";
  elif [ $kafka_site_start_date = "null" ]
  then
    start_date_trino=$START_DATE;
    end_date_trino=$END_DATE;
    unset start_date_kafka;
    unset end_date_kafka;
    echo "No kafka start date is listed for $site. Setting trino data only from $start_date_trino to $end_date_trino (inclusive).";
  elif [ $(date -u +%s -d $kafka_site_start_date) -gt $(date -u +%s -d $START_DATE) ] && [ $(date -u +%s -d $END_DATE) -gt $(date -u +%s -d $kafka_site_start_date) ]
  then
    start_date_trino=$START_DATE;
    end_date_trino=$(date -u +%Y-%m-%d -d "$kafka_site_start_date -1 day");
    start_date_kafka=$kafka_site_start_date;
    end_date_kafka=$END_DATE;
    echo "Kafka site start date is after input START_DATE ($START_DATE) and before END_DATE. Setting trino data from $start_date_trino to $end_date_trino and kafka data from $start_date_kafka to $end_date_kafka (inclusive) for $site.";
  elif [ $(date -u +%s -d $kafka_site_start_date) -gt $(date -u +%s -d $START_DATE) ] && ! [ $(date -u +%s -d $END_DATE) -gt $(date -u +%s -d $kafka_site_start_date) ]
  then
    start_date_trino=$START_DATE;
    end_date_trino=$END_DATE;
    unset start_date_kafka;
    unset end_date_kafka;
    echo "Kafka start date ($kafka_site_start_date) is listed for $site but is after END_DATE ($END_DATE). Setting only trino data from $start_date_trino to $end_date_trino (inclusive).";
  else
    unset start_date_trino;
    unset end_date_trino;
    start_date_kafka=$START_DATE;
    end_date_kafka=$END_DATE;
    echo "Input START_DATE ($START_DATE) is after kafka site start date. Setting only kafka data from $start_date_kafka to $end_date_kafka (inclusive) for $site.";
  fi
  
  # Create daily folder structure for trino data
  echo "Creating import trigger folder structure for $site"
  if ! [ -z $start_date_trino ]
  then
    for d in $(seq $(date -u +%s -d $start_date_trino) +86400 $(date -u +%s -d $end_date_trino)) ; do
      date_path=$(date -u +%Y/%m/%d -d @$d);
      mkdir -p $OUT_PATH/$SOURCE_TYPE/$date_path;
      touch $OUT_PATH/$SOURCE_TYPE/$date_path/$site;
    done;
  fi
  
  # Create daily folder structure for kafka data
  if ! [ -z $start_date_kafka ]
  then
    for d in $(seq $(date -u +%s -d $start_date_kafka) +86400 $(date -u +%s -d $end_date_kafka)) ; do
    date_path=$(date -u +%Y/%m/%d -d @$d);
    mkdir -p $OUT_PATH/$SOURCE_TYPE/$date_path;
    touch $OUT_PATH/$SOURCE_TYPE/$date_path/$site.kafka;
    done;
  fi
  done
  
  # Finally, create a file listing the data years (for the metadata assignment pipelines)
  ls $OUT_PATH/$SOURCE_TYPE > $OUT_PATH/data_years.txt