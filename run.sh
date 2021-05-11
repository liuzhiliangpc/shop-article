#!/bin/bash
#refer:https://docs.docker.com/config/containers/multi-service_container/

# all server names
server_names=(
'server.web_celery:app'
'celery'
)


# all server start bash commands
start_commands=(
`nohup gunicorn -c gunicorn_config.py 'server.web_celery:app' > /log/shop_article_server 2>&1 &`
`nohup celery -A async_tasks.celery worker --concurrency=2 --loglevel=info -P gevent > /log/shop_article_async_task 2>&1 &`
)


log_filenames=(
'/log/shop_article_server'
'/log/shop_article_async_task'
)


len=${#server_names[@]}
for ((i=0;i<$len;i++))
    do
        ${start_commands[$i]}
        PROCESS_STATUS=$?
        if [ $PROCESS_STATUS -ne 0 ]; then
            echo "Failed to start ${server_names[$i]}, please see error log"
             echo `cat ${log_filenames[$i]} | tail -n 150`
            exit PROCESS_STATUS
        fi
    done



# Naive check runs checks once a minute to see if either of the processes exited.
# This illustrates part of the heavy lifting you need to do if you want to run
# more than one service in a container. The container exits with an error
# if it detects that either of the processes has exited.
# Otherwise it loops forever, waking up every 60 seconds

while sleep 30; do
    for ((i=0;i<$len;i++))
        do
            ps aux |grep ${server_names[$i]} |grep -q -v grep
            PROCESS_STATUS=$?
            if [ $PROCESS_STATUS -ne 0 ]; then
                echo "ops~~ : ${server_names[$i]} has already exited, please see error log"
                echo `cat ${log_filenames[$i]} | tail -n 150`
                exit 1
            fi
        done
done
