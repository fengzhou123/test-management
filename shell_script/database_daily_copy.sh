#!/bin/sh

currDate=`date '+%Y%m%d'`

/src/kiwi_9.40.1_01oct2020/bin/dbcopy nzvader:kdw_9741_master_datawarehouse nzvader:9741bk_${currDate}_master_datawarehouse

/src/kiwi_9.40.1_01oct2020/bin/dbcopy nzvader:kdw_9741_working_datawarehouse nzvader:9741bk_${currDate}_working_datawarehouse

echo "It triggers a Jenkins project"
