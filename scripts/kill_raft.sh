#!/usr/bin/env bash

# Deduce target jar
ARTIFACT=$(mvn help:evaluate -Dexpression=project.artifactId -q -DforceStdout)
VERSION=$(mvn help:evaluate -Dexpression=project.version -q -DforceStdout)
JAR_NAME="target/${ARTIFACT}-${VERSION}.jar"

# The pattern to match.
PATTERN="java -jar ${JAR_NAME}"

echo "Killing processes matching pattern: $PATTERN"

# Send a TERM signal (graceful shutdown) to all matching processes.
pkill -f "$PATTERN"
