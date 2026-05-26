#!/bin/bash
# Script to generate all keystores and truststore for the project
# Run this script from the project root directory

TRUSTSTORE_FILE="client-truststore.jks"
TRUSTSTORE_PWD="changeit"
KEYSTORE_PWD="changeit"
VALIDITY=365

# Remove old files
rm -f *.jks

# Server names and their keystore filenames
declare -A SERVERS
SERVERS["users.ourorg0"]="users-0.jks"
SERVERS["messages0.ourorg0"]="messages0-0.jks"
SERVERS["messages1.ourorg0"]="messages1-0.jks"
SERVERS["messages2.ourorg0"]="messages2-0.jks"
SERVERS["users.ourorg1"]="users-1.jks"
SERVERS["messages0.ourorg1"]="messages0-1.jks"
SERVERS["messages1.ourorg1"]="messages1-1.jks"
SERVERS["messages2.ourorg1"]="messages2-1.jks"
SERVERS["users.ourorg2"]="users-2.jks"
SERVERS["messages0.ourorg2"]="messages0-2.jks"

# Generate keystores and add certificates to truststore
for SERVER in "${!SERVERS[@]}"; do
    KEYSTORE="${SERVERS[$SERVER]}"
    echo "Generating keystore for $SERVER -> $KEYSTORE"
    
    keytool -genkeypair \
        -alias "$SERVER" \
        -keyalg RSA \
        -keysize 2048 \
        -validity $VALIDITY \
        -keystore "$KEYSTORE" \
        -storepass "$KEYSTORE_PWD" \
        -keypass "$KEYSTORE_PWD" \
        -dname "CN=$SERVER" \
        -ext "SAN=dns:$SERVER" \
        -storetype JKS

    # Export certificate
    keytool -exportcert \
        -alias "$SERVER" \
        -keystore "$KEYSTORE" \
        -storepass "$KEYSTORE_PWD" \
        -file "${SERVER}.cer" \
        -rfc

    # Import into truststore
    keytool -importcert \
        -alias "$SERVER" \
        -file "${SERVER}.cer" \
        -keystore "$TRUSTSTORE_FILE" \
        -storepass "$TRUSTSTORE_PWD" \
        -noprompt \
        -storetype JKS

    # Clean up certificate file
    rm -f "${SERVER}.cer"
done

echo ""
echo "Generated keystores:"
ls -la *.jks
echo ""
echo "Truststore: $TRUSTSTORE_FILE"
echo "Truststore password: $TRUSTSTORE_PWD"
echo "All keystore passwords: $KEYSTORE_PWD"
