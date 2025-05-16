# kafka-dump

Kafka consumer that read data from kafka and write them to an aws s3 bucket
splited by topic, date every 5 minutes

# Environment variables

    APP_KAFKA_BROKERS
    APP_KAFKA_TOPICS
    APP_KAFKA_GROUPID
    APP_S3_BUCKETPREFIX
    AWS_ACCESS_KEY_ID
    AWS_SECRET_ACCESS_KEY
    AWS_DEFAULT_REGION
