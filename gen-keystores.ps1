# Script to generate all keystores and truststore for the project
# Run from the project root directory (Sd-Trab1/)

$TRUSTSTORE_FILE = "client-truststore.jks"
$TRUSTSTORE_PWD = "changeit"
$KEYSTORE_PWD = "changeit"
$VALIDITY = 365

# Remove old files
Remove-Item -Path "*.jks" -ErrorAction SilentlyContinue
Remove-Item -Path "*.cer" -ErrorAction SilentlyContinue

# Server -> keystore mapping
$SERVERS = @{
    "users.ourorg0"     = "users-0.jks"
    "messages0.ourorg0" = "messages0-0.jks"
    "messages1.ourorg0" = "messages1-0.jks"
    "messages2.ourorg0" = "messages2-0.jks"
    "users.ourorg1"     = "users-1.jks"
    "messages0.ourorg1" = "messages0-1.jks"
    "messages1.ourorg1" = "messages1-1.jks"
    "messages2.ourorg1" = "messages2-1.jks"
    "users.ourorg2"     = "users-2.jks"
    "messages0.ourorg2" = "messages0-2.jks"
}

foreach ($server in $SERVERS.Keys) {
    $keystore = $SERVERS[$server]
    Write-Host "Generating keystore for $server -> $keystore"

    # Generate keypair
    & keytool -genkeypair `
        -alias $server `
        -keyalg RSA `
        -keysize 2048 `
        -validity $VALIDITY `
        -keystore $keystore `
        -storepass $KEYSTORE_PWD `
        -keypass $KEYSTORE_PWD `
        -dname "CN=$server" `
        -ext "SAN=dns:$server" `
        -storetype JKS 2>&1 | Out-Null

    # Export certificate
    & keytool -exportcert `
        -alias $server `
        -keystore $keystore `
        -storepass $KEYSTORE_PWD `
        -file "$server.cer" `
        -rfc 2>&1 | Out-Null

    # Import into truststore
    & keytool -importcert `
        -alias $server `
        -file "$server.cer" `
        -keystore $TRUSTSTORE_FILE `
        -storepass $TRUSTSTORE_PWD `
        -noprompt `
        -storetype JKS 2>&1 | Out-Null

    # Clean up
    Remove-Item -Path "$server.cer" -ErrorAction SilentlyContinue
}

Write-Host ""
Write-Host "Generated keystores:"
Get-ChildItem *.jks | Format-Table Name, Length
Write-Host "Truststore: $TRUSTSTORE_FILE"
Write-Host "All passwords: $KEYSTORE_PWD"
