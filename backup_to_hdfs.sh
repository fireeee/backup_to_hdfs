#!/bin/bash

USER=iivanov
BACKUP_DIR=/user/$USER/backup_directory
RET_DAYS=30

upload_to_hdfs () {
hadoop fs -mkdir $BACKUP_DIR
hadoop fs -rm $BACKUP_DIR/result.csv
hadoop fs -rm $BACKUP_DIR/$(date +%Y-%m-%d)_result.csv
hadoop fs -copyFromLocal result.csv $BACKUP_DIR
hadoop fs -mv $BACKUP_DIR/result.csv $BACKUP_DIR/$(date +%Y-%m-%d)_result.csv


}

cleanup_old_from_hdfs () {
hadoop fs -ls -R $BACKUP_DIR/ | grep  "^-" | tr -s " " | cut -d' ' -f6-8 | awk 'BEGIN{ RETENTION_DAYS=$RET_DAYS; LAST=24*60*60*RETENTION_DAYS; "date +%s" | getline NOW } { cmd="date -d'\''"$1" "$2"'\'' +%s"; cmd | getline WHEN; DIFF=NOW-WHEN; if(DIFF > LAST){ system("hadoop fs -rm -r -skipTrash "$3 ) }}'

}

upload_to_hdfs
cleanup_old_from_hdfs

