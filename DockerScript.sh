#!/bin/sh
export AWSACCESSKEYID=${AWS_ACCESS_KEY_ID}
export AWSSECRETACCESSKEY=${AWS_SECRET_ACCESS_KEY}
export KAFKABROKER=${KAFKA_BROKER}
export TOPIC=${KAFKA_TOPIC}
export S3PATH=${PATH2SAVE}

spark-submit --driver-java-options "-Dlog4j.configuration=file:log4j.properties" --packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.0.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0,jp.co.bizreach:aws-s3-scala_2.12:0.0.15,org.apache.hadoop:hadoop-common:3.2.0,org.apache.hadoop:hadoop-aws:3.2.0,com.amazonaws:aws-java-sdk-bundle:1.11.375 --class com.twitterETL.TwitterETL  "./target/scala-2.12/twitter_docker_s3_2.12-0.1.jar" ${KAFKABROKER} ${TOPIC} "./src/main/Resources/twitterSchema.json" ${AWSACCESSKEYID} ${AWSSECRETACCESSKEY} ${S3PATH}
