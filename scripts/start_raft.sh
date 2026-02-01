#!/usr/bin/env bash

# Deduce target jar
ARTIFACT=$(mvn help:evaluate -Dexpression=project.artifactId -q -DforceStdout)
VERSION=$(mvn help:evaluate -Dexpression=project.version -q -DforceStdout)
JAR_NAME="target/${ARTIFACT}-${VERSION}.jar"

# Array of ports for the five nodes
PORTS=(10080 10081 10082 10083 10084)

# Launch each node
for ((i=0; i<${#PORTS[@]}; i++)); do
    MY_PORT="${PORTS[$i]}"
    PEERS=()

    # Gather all other ports as peers
    for ((j=0; j<${#PORTS[@]}; j++)); do
        if [ "$i" -ne "$j" ]; then
            PEERS+=("${PORTS[$j]}")
        fi
    done

    echo "Starting Raft node on port $MY_PORT with peers ${PEERS[*]}"

    # Run the jar in the background, capturing logs in a separate file if desired
    # java -jar "$JAR_NAME" "$MY_PORT" "${PEERS[@]}" \
    #    > "raft_${MY_PORT}.log" 2>&1 &
    java -jar "${JAR_NAME}" "$MY_PORT" "${PEERS[@]}" 2>&1 &
done

echo "All Raft nodes launched."
echo "------------------------"

ps -ef | grep "${JAR_NAME}"
