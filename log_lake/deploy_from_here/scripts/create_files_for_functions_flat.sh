#!/bin/bash
read -p "Enter the full path to your addapart folder (such as /Users/user/Desktop/colins_laptop/everything/repos/LogLakeProofOfConcept/for_blog/log_lake/src/pipelines/add_a_part): " SRC_FOLDER
read -p "Enter the full path to your target resource archives (such as /Users/colicars/Desktop/colins_laptop/everything/repos/LogLakeProofOfConcept/for_blog/log_lake/deploy_from_here/resources): " RESOURCE_FOLDER
zip -j "${RESOURCE_FOLDER}"/files_for_producer_raw.zip "${SRC_FOLDER}"/producer/lambda_function.py "${SRC_FOLDER}"/conf/raw/conf.py
zip -j "${RESOURCE_FOLDER}"/files_for_consumer_raw.zip "${SRC_FOLDER}"/consumer/lambda_function.py "${SRC_FOLDER}"/conf/raw/conf.py
zip -j "${RESOURCE_FOLDER}"/files_for_producer_readready.zip "${SRC_FOLDER}"/producer/lambda_function.py "${SRC_FOLDER}"/conf/readready/conf.py
zip -j "${RESOURCE_FOLDER}"/files_for_consumer_readready.zip "${SRC_FOLDER}"/consumer/lambda_function.py "${SRC_FOLDER}"/conf/readready/conf.py
