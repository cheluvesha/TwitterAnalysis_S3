FROM 051c82900e82
USER root

WORKDIR /app
COPY . /app/
COPY DockerScript.sh .
RUN chmod +x /app/DockerScript.sh

ENV KAFKA_BROKER default
ENV KAFKA_TOPIC default
ENV TWEET_SAMPLE_FILE default
ENV AWS_ACCESS_KEY_ID default
ENV AWS_SECRET_ACCESS_KEY default
ENV S3PATH default

RUN sbt update
RUN sbt clean package

ENTRYPOINT ["/app/DockerScript.sh"]
