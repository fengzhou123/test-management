#!/usr/bin/env bash

sed -i s/PASS/Passed/g $1
sed -i s/FAIL/Failed/g $1

testCaseNameArray=($(awk -F'"' '/testcase/ { print $6}' $1))
testStatusArray=($(awk -F'"' '/testcase/ { print $8}' $1))

testExecListStr=`curl -X POST "https://apiss.kualitee.com/api/v2/test_case_execution/list" -H "accept: application/json" -H "Content-Type: application/x-www-form-urlencoded" -d "token=de8859a5c56612b4cf84afc307e32456&project_id=9476&build_id=14157&cycle_id=10083"`

printf "This is the testExecListStr\n%s\n" "${testExecListStr}"

#testCaseIdArray=grep -oP '(?<="testcase_id":")\d+(?=")' testExecListStr

for i in "${!testCaseNameArray[@]}"
do
  testCaseName=${testCaseNameArray[$i]}
  testStatus=${testStatusArray[$i]}		

  printf "%s\t%s\n" "${testCaseName}" "${testStatus}"

  testCaseId=`echo ${testExecListStr} | grep -oP "(?<=\"testcase_id\":\")\d+(?=\",\"cycle_id\":\"10083\",\"id\":\"\d+\",\"build_id\":\"14157\",\"testscenario_id\":\"\d+\",\"project_id\":\"9476\",\"buid\":\"14157\",\"cycid\":\"10083\",\"tcid\":\"\d+\",\"exec_type\":\"[a-zA-Z]+\",\"tc_name\":\"${testCaseName}\")"`

  echo "testCaseId ${testCaseId}" 

  curl -X POST "https://apiss.kualitee.com/api/v2/test_case_execution/execute" -H "accept: application/json" -H "Content-Type: application/x-www-form-urlencoded" -d "token=de8859a5c56612b4cf84afc307e32456&project_id=9476&status=${testStatus}&tc_id=${testCaseId}&cycle_id=10083&build_id=14157&notes=Result%20upload%20via%20API&execute=yes"

printf "\n"

done

