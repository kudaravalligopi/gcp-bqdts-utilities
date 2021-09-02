#!/usr/bin/env bash
#
# BQDTS generic utility shell functions to help td2bq transfers

bqdts_config_init() {
	# Load configuration, and setup environment variables
    # Set the PROJECT variable
    # export PROJECT=$(gcloud config get-value project)
    # Set TD2BQ_SVC_ACCOUNT = service account email
    # export TD2BQ_SVC_ACCOUNT=td2bq-transfer@${PROJECT}.iam.gserviceaccount.com
}

get_credentials() {
	# TODO: Extract needed credentials
	# 
	# Script to get the service account key/credentials from the vault is called here.
	# Vault related code is explicitly kept as TODO here for security reasons
	# credentials need to be made available as parameter, and/or environment variables.
	# 	
}

create_dts_transfer() {
        # Function to create on-demand BQDTS transfer job configuration
        # Your GCP project ID. If not supplied, default will be used
        local PROJECT_ID="$1"
        # Dataset you wish to target for the transfer configuration
        local TARGET_BQ_DATASET="$2"
        # Display name for the transfer configuration
        local TRANSFER_NAME="$3"
        # Cloud Storage bucket that will act as a staging area during the migration
        local STAGING_GCS_BUCKET="$4"
        # Default to Teradata for TD2BQ migrations
        local DATABASE_TYPE="Teradata"
        # Source DB name in Teradata
        local TD_DATABASE_NAME="$5"
        # Pattern(s) for matching the table names in the source database. You can use regular expressions to specify the pattern
        # Valid patterns: 
        # Pipe delimited list of tables: tpch.lineitem|part|orders OR
        # Semicolon delimited list of qualified table names: tpch.lineitem;tpch.orders OR
        # Use a wildcard: tpch..*
        local TD_TABLE_NAME_PATTERNS="$6"
        # Email address associated with the service account marked for DTS
        local AGENT_SERVICE_ACCOUNT_EMAIL=${TD2BQ_SVC_ACCOUNT}
        # Path to custom schema JSON file. Check sample in the code base
        local CUSTOM_ANNOTATED_SCHEMA_GCS_FILE_PATH="$7"
        local DATA_SOURCE="on_premises"
        # For Teradata migrations, these parameters are required: bucket, database_type, agent_service_account, database_name, table_name_patterns
        { bq mk \
                --transfer_config \
                --project_id=${PROJECT_ID} \
                --target_dataset=$TARGET_BQ_DATASET \
                --display_name=${TRANSFER_NAME} \
                # Contains the parameters (--params) for the created transfer configuration in JSON format. For example: --params='{"param":"param_value"}' \
                --params='{ \
                    "bucket": ${STAGING_GCS_BUCKET}, \
                    "database_type": ${DATABASE_TYPE}, \
                    "database_name":${TD_DATABASE_NAME}, \
                    "table_name_patterns": ${TD_TABLE_NAME_PATTERNS},\
                    "agent_service_account": ${AGENT_SERVICE_ACCOUNT_EMAIL}, \
                    "schema_file_path": ${CUSTOM_ANNOTATED_SCHEMA_GCS_FILE_PATH}
                    }' \
                --data_source=${DATA_SOURCE} && return 0; } || return 1
}


initialize_migration_agent() {
    # Function to initialize migration agent to generate a configuration file one-time
    # This script prompts for additional inputs including 
    # On the subsequent prompts:
    # Database hostname: use the Teradata host IP 
    # Database port: provide the Teradata port 
    # Path to extracted files: /tmp/extracted
    # BigQuery Data Transfer Service resource name - type in the Resource Name from the previous step create_dts_transfer().
    # Configuration file name: config
    # Path to optional database-credentials-file to avoid prompt while running the agent
    # TODO: Create expect script for automating prompted parameters
    # Path to migration agent and Teradata JDBC jars
    # Example: /usr/local/migration/Teradata/JDBC/tdgssconfig.jar
    local TDGSSCONFIG_JAR_PATH="$1"
    # Example: /usr/local/migration/Teradata/JDBC/terajdbc4.jar
    local TERAJDBC4_JAR_PATH="$2"
    # Example: /usr/local/migration/mirroring-agent.jar
    local MIRRORING_AGENT_JAR_PATH="$3"
        { java -cp \
                ${TDGSSCONFIG_JAR_PATH}:${TERAJDBC4_JAR_PATH}:${MIRRORING_AGENT_JAR_PATH} \
                com.google.cloud.bigquery.dms.Agent \
                --initialize
}

initialize_migration_agent_expect() {
    # Optional function to initialize migration agent to generate a configuration file without additional prompts
    # TODO: Create expect script for automating prompted parameters
}

initialize_migration_agent_create_schema() {
    # Optional function to initialize migration agent to save JSON schema from Teradata
    # TODO: Create expect script for automating prompted parameters during initialization
}

run_migration_agent() {
    # Function to run migration agent using the generated configuration file from initilization step.
    # Ensure the flag that passes a credentials file to the agent, instead of entering the Teradata username and password each time. 
    # Set the optional parameter database-credentials-file-path in the agent configuration file to avoid prompt
    # Path to migration agent and Teradata JDBC jars
    # Example: /usr/local/migration/Teradata/JDBC/tdgssconfig.jar
    local TDGSSCONFIG_JAR_PATH="$1"
    # Example: /usr/local/migration/Teradata/JDBC/terajdbc4.jar
    local TERAJDBC4_JAR_PATH="$2"
    # Example: /usr/local/migration/mirroring-agent.jar
    local MIRRORING_AGENT_JAR_PATH="$3"
    # Configuration file path generated during agent initialization
    local CONFIG_JSON_FILE_PATH="$4"
        { java -cp \
                ${TDGSSCONFIG_JAR_PATH}:${TERAJDBC4_JAR_PATH}:${MIRRORING_AGENT_JAR_PATH} \
                com.google.cloud.bigquery.dms.Agent \
                --configuration-file=${CONFIG_JSON_FILE_PATH}


}
