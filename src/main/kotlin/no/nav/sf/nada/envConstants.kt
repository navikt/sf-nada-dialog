package no.nav.sf.nada

const val config_MAPDEF_FILE = "MAPDEF_FILE"
const val config_GCP_TEAM_PROJECT_ID = "GCP_TEAM_PROJECT_ID"
const val config_POST_TO_BIGQUERY = "POST_TO_BIGQUERY"
const val config_EXCLUDE_TABLES = "EXCLUDE_TABLES"

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

fun env(name: String): String = System.getenv(name) ?: throw NullPointerException("Missing env $name")

fun envAsBoolean(env: String): Boolean { return System.getenv(env).trim().toBoolean() }

fun envAsList(env: String): List<String> { return System.getenv(env).split(",").map { it.trim() }.toList() }
