package no.nav.sf.nada

import com.google.gson.Gson
import com.google.gson.JsonArray
import com.google.gson.JsonObject
import com.google.gson.JsonParser
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import mu.KotlinLogging
import no.nav.kafka.dialog.ShutdownHook
import no.nav.sf.nada.token.AccessTokenHandler
import org.apache.commons.csv.CSVFormat
import org.apache.commons.csv.CSVParser
import org.http4k.core.Method
import org.http4k.core.Request
import org.http4k.core.Response
import org.http4k.urlEncoded
import java.io.File
import java.io.StringReader
import java.time.LocalDate
import java.time.format.DateTimeFormatter

private val log = KotlinLogging.logger { }

val gson = Gson()

/**
 * conditionalWait
 * Interruptable wait function
 */
fun conditionalWait(ms: Long) =
    runBlocking {

        log.debug { "Will wait $ms ms" }

        val cr = launch {
            runCatching { delay(ms) }
                .onSuccess { log.debug { "waiting completed" } }
                .onFailure { log.info { "waiting interrupted" } }
        }

        tailrec suspend fun loop(): Unit = when {
            cr.isCompleted -> Unit
            ShutdownHook.isActive() -> cr.cancel()
            else -> {
                delay(250L)
                loop()
            }
        }

        loop()
        cr.join()
    }

/**
 * Shortcuts for fetching environment variables
 */
fun env(env: String): String { return System.getenv(env) }

fun envAsBoolean(env: String): Boolean { return System.getenv(env).trim().toBoolean() }

fun envAsList(env: String): List<String> { return System.getenv(env).split(",").map { it.trim() }.toList() }

enum class Type { STRING, INTEGER, DATETIME, DATE, BOOL }
data class FieldDef(val name: String, val type: Type)
data class TableDef(val query: String, val fieldDefMap: MutableMap<String, FieldDef>)

fun parseMapDef(filePath: String): Map<String, Map<String, TableDef>> =
    parseMapDef(JsonParser.parseString(Bootstrap::class.java.getResource(filePath).readText()) as JsonObject)

fun parseMapDef(obj: JsonObject): Map<String, Map<String, TableDef>> {
    val result: MutableMap<String, MutableMap<String, TableDef>> = mutableMapOf()

    obj.entrySet().forEach { dataSetEntry ->
        val objDS = dataSetEntry.value.asJsonObject
        result[dataSetEntry.key] = mutableMapOf()
        objDS.entrySet().forEach { tableEntry ->
            val objT = tableEntry.value.asJsonObject
            val query = objT["query"]!!.asString.replace(" ", "+")
            val objS = objT["schema"]!!.asJsonObject
            result[dataSetEntry.key]!![tableEntry.key] = TableDef(query, mutableMapOf())
            objS.entrySet().forEach { fieldEntry ->
                val fieldDef = gson.fromJson(fieldEntry.value, FieldDef::class.java)
                result[dataSetEntry.key]!![tableEntry.key]!!.fieldDefMap[fieldEntry.key] = fieldDef
            }
        }
    }
    return result
}

fun doSFQuery(query: String): Response {
    val request = Request(Method.GET, "$query")
        .header("Authorization", "Bearer ${AccessTokenHandler.accessToken}")
        .header("Content-Type", "application/json;charset=UTF-8")
    // File("/tmp/queryToHappen").writeText(request.toMessage())
    val response = Bootstrap.client.value(request)
    // File("/tmp/responseThatHappend").writeText(response.toMessage())
    return response
}

fun doSFBulkStartQuery(dataset: String, table: String): Response {
    val query = Bootstrap.mapDef[dataset]!![table]!!.query.replace("+", " ")
    val request = Request(Method.POST, "${AccessTokenHandler.instanceUrl}/services/data/v57.0/jobs/query")
        .header("Authorization", "Bearer ${AccessTokenHandler.accessToken}")
        .header("Content-Type", "application/json;charset=UTF-8")
        .body(
            """{
                "operation": "query",
                "query": "$query",
                "contentType": "CSV"
                  }""".trim()
        )

    File("/tmp/bulkQueryToHappen").writeText(request.toMessage())
    val response = Bootstrap.client.value(request)
    File("/tmp/bulkResponseThatHappend").writeText(response.toMessage())
    return response
}

fun doSFBulkJobStatusQuery(jobId: String): Response {
    val request = Request(Method.GET, "${AccessTokenHandler.instanceUrl}/services/data/v57.0/jobs/query/$jobId")
        .header("Authorization", "Bearer ${AccessTokenHandler.accessToken}")
        .header("Content-Type", "application/json;charset=UTF-8")
    File("/tmp/bulkJobStatusQueryToHappen").writeText(request.toMessage())
    val response = Bootstrap.client.value(request)
    File("/tmp/bulkJobStatusResponseThatHappend").writeText(response.toMessage())
    return response
}

fun doSFBulkJobResultQuery(jobId: String, locator: String? = null): Response {
    // GET /services/data/v57.0/jobs/query/<jobID>/results
    val request = Request(Method.GET, "${AccessTokenHandler.instanceUrl}/services/data/v57.0/jobs/query/$jobId/results${locator?.let{"?locator=$locator"} ?: ""}")
        .header("Authorization", "Bearer ${AccessTokenHandler.accessToken}")

    val response = Bootstrap.client.value(request)
    File("/tmp/bulkJobResultResponse${locator?.let{"-$locator"} ?: ""}").writeText(response.toMessage())
    return response
}

fun String.addDateRestriction(localDate: LocalDate): String {
    val connector = if (this.contains("WHERE")) "+AND" else "+WHERE"
    return this + "$connector+LastModifiedDate>=TODAY+AND+LastModifiedDate<=TOMORROW"
        .replace("TODAY", "${localDate.format(DateTimeFormatter.ISO_DATE)}T00:00:00Z".urlEncoded())
        .replace("TOMORROW", "${localDate.plusDays(1).format(DateTimeFormatter.ISO_DATE)}T00:00:00Z".urlEncoded())
        .replace(">", ">".urlEncoded())
        .replace("<", "<".urlEncoded())
}

fun String.addYesterdayRestriction(): String = this.addDateRestriction(LocalDate.now().minusDays(1))

fun parseCSVToJsonArrays(csvData: String): List<JsonArray> {
    File("/tmp/csvData").writeText(csvData)
    val listOfJsonArrays: MutableList<JsonArray> = mutableListOf()
    val rowLimit = 500
    var jsonArray = JsonArray()

    // Parse the CSV data with the new approach for headers
    val reader = StringReader(csvData)
    val csvParser = CSVParser(
        reader,
        CSVFormat.DEFAULT.builder().setSkipHeaderRecord(true).setHeader().build()
    )

    // Iterate through the records (skipping the header row)
    for (csvRecord in csvParser) {
        val jsonObject = JsonObject()

        // For each column in the record, add the key-value pair to the JsonObject
        csvRecord.toMap().forEach { (key, value) ->
            jsonObject.addProperty(key, if (value.isNullOrBlank()) null else value)
        }

        if (jsonArray.size() == rowLimit) {
            listOfJsonArrays.add(jsonArray)
            jsonArray = JsonArray()
        }
        // Add the JsonObject to the JsonArray
        jsonArray.add(jsonObject)
    }
    listOfJsonArrays.add(jsonArray)

    // Close the reader and parser
    csvParser.close()
    reader.close()

    return listOfJsonArrays
}
