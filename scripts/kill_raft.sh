#!/usr/bin/env bash

# Deduce target jar
ARTIFACT=$(mvn help:evaluate -Dexpression=project.artifactId -q -DforceStdout)
VERSION=$(mvn help:evaluate -Dexpression=project.version -q -DforceStdout)
JAR_NAME="target/${ARTIFACT}-${VERSION}.jar"

# Match java processes running this jar even when JVM properties precede -jar.
PATTERN="java .* -jar ${JAR_NAME}"
TELEMETRY_PATTERN="java .* -jar ${JAR_NAME} telemetry"
LOG_DIR="${RAFT_LOG_DIR:-./raft-demo}"
SUMMARY_PID_FILE="${LOG_DIR}/cluster-summary.pid"

echo "Killing processes matching pattern: $PATTERN"

# Send a TERM signal (graceful shutdown) to all matching processes.
pkill -f "$PATTERN" || true
pkill -f "$TELEMETRY_PATTERN" || true

if [[ -f "${SUMMARY_PID_FILE}" ]]; then
    SUMMARY_PID="$(cat "${SUMMARY_PID_FILE}")"
    if [[ -n "${SUMMARY_PID}" ]]; then
        kill "${SUMMARY_PID}" 2>/dev/null || true
    fi
    rm -f "${SUMMARY_PID_FILE}"
fi
