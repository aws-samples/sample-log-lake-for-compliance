#!/bin/bash
read -p "Enter the full path to your blog folder (such as /Users/someuser/Desktop/my_laptop/everything/repos/LogLakeProofOfConcept/for_blog): " BLOG_FOLDER
cd "${BLOG_FOLDER}"/log_lake/src/lambda
zip -j "${BLOG_FOLDER}"/log_lake/deploy_from_here/resources/files_for_looker_lambda.zip looker/lambda_function.py looker/prompt_preparation_module.py looker/query_manager_module.py looker/talk_to_rock.py  
