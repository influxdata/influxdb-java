#!/usr/bin/env bash

wget https://github.com/google/google-java-format/releases/download/google-java-format-1.4/google-java-format-1.4-all-deps.jar

JAVA_FILES=$(find src/ -name "*.java")

for JAVA_FILE in ${JAVA_FILES}
do
 echo "formatting ${JAVA_FILE}"
 docker run -it --rm \
	-v $PWD:/mnt \
        openjdk java -jar /mnt/google-java-format-1.4-all-deps.jar -r /mnt/${JAVA_FILE}
done
