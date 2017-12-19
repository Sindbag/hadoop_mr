#!/usr/bin/env bash

if [ $# -ge 1 ]; then
    TODAY=$1
else
    TODAY=$(/home/abardukov/mr/helpers/today.py)
fi

echo "GOING FOR ${TODAY}"

/home/abardukov/mr/tasks/easy_base_metrics.sh ${TODAY}
echo 'done easy metrics'

/home/abardukov/mr/tasks/top10_pages_metrics.sh ${TODAY}
echo 'done daily top10 urls'

/home/abardukov/mr/tasks/backlog_users_prepare.sh ${TODAY}
echo 'prepared backlog for users'

/home/abardukov/mr/tasks/count_new_users.sh ${TODAY}
echo 'counted new, lost and converted users'
