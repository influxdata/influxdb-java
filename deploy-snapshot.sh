#!/usr/bin/env bash

set -e

#Parse project version from pom.xml
export PROJECT_VERSION=`xmllint --xpath "//*[local-name()='project']/*[local-name()='version']/text()" pom.xml`
echo "Project version: $PROJECT_VERSION"

#Skip if not *SNAPSHOT
if [[ $PROJECT_VERSION != *SNAPSHOT ]]; then
    echo "$PROJECT_VERSION is not SNAPSHOT - skip deploy.";
    exit;
fi


DEFAULT_MAVEN_JAVA_VERSION="3-jdk-8-slim"
MAVEN_JAVA_VERSION="${MAVEN_JAVA_VERSION:-$DEFAULT_MAVEN_JAVA_VERSION}"
echo "Deploy snapshot with maven:${MAVEN_JAVA_VERSION}"

docker run -it --rm \
       --volume ${PWD}:/usr/src/mymaven \
       --volume ${PWD}/.m2:/root/.m2 \
       --workdir /usr/src/mymaven \
       --env SONATYPE_USERNAME=${SONATYPE_USERNAME} \
       --env SONATYPE_PASSWORD=${SONATYPE_PASSWORD} \
       maven:${MAVEN_JAVA_VERSION} mvn -s .maven-settings.xml -DskipTests=true clean package deploy
