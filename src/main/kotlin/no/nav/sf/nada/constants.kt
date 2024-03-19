package no.nav.sf.nada

const val env_DEPLOY_APP = "DEPLOY_APP"
const val env_DEPLOY_CLUSTER = "DEPLOY_CLUSTER"

const val env_MAPDEF_FILE = "MAPDEF_FILE"

const val SF_PATH_oAuth = "/services/oauth2/token"

const val env_GCP_TEAM_PROJECT_ID = "GCP_TEAM_PROJECT_ID"

const val env_POST_TO_BIGQUERY = "POST_TO_BIGQUERY"

const val env_RUN_SESSION_ON_STARTUP = "RUN_SESSION_ON_STARTUP"

const val env_FETCH_ALL_RECORDS = "FETCH_ALL_RECORDS"

const val env_EXCLUDE_TABLES = "EXCLUDE_TABLES"

// Salesforce environment dependencies
const val env_SF_TOKENHOST = "SF_TOKENHOST"
const val env_SF_QUERY_BASE = "SF_QUERY_BASE"

// Salesforce required secrets
const val secret_SFClientID = "SFClientID"
const val secret_SFUsername = "SFUsername"

// Salesforce required secrets related to keystore for signed JWT
const val secret_keystoreJKSB64 = "keystoreJKSB64"
const val secret_KeystorePassword = "KeystorePassword"
const val secret_PrivateKeyAlias = "PrivateKeyAlias"
const val secret_PrivateKeyPassword = "PrivateKeyPassword"
