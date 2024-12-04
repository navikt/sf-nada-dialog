package no.nav.sf.nada.gui

import com.google.cloud.bigquery.BigQuery
import com.google.cloud.bigquery.StandardTableDefinition
import com.google.cloud.bigquery.Table
import com.google.gson.JsonObject
import com.google.gson.JsonParser
import mu.KotlinLogging
import no.nav.sf.nada.HttpCalls.doSFQuery
import no.nav.sf.nada.Metrics
import no.nav.sf.nada.TableDef
import no.nav.sf.nada.addLimitRestriction
import no.nav.sf.nada.addYesterdayRestriction
import no.nav.sf.nada.application
import no.nav.sf.nada.bulk.BulkOperation
import no.nav.sf.nada.bulk.OperationInfo
import no.nav.sf.nada.gson
import no.nav.sf.nada.token.AccessTokenHandler
import org.http4k.core.HttpHandler
import org.http4k.core.Request
import org.http4k.core.Response
import org.http4k.core.Status
import java.io.File

typealias BigQueryMetadata = Map<String, Gui.DatasetMetadata>

object Gui {
    private val log = KotlinLogging.logger { }

    val metaDataHandler: HttpHandler = {
        val response =
            try {
                Response(Status.OK).body(gson.toJson(fetchBigQueryMetadata(application.mapDef)))
            } catch (e: java.lang.Exception) {
                Response(Status.INTERNAL_SERVER_ERROR).body(e.stackTraceToString())
            }
        response
    }

    val testCallHandler: HttpHandler = { req: Request ->
        File("/tmp/latesttestcallrequest").writeText(req.toMessage())
        var result = ""
        val dataset = req.query("dataset")
        val table = req.query("table")
        log.info { "Will perform testcall $dataset $table" }
        val query = application.mapDef[dataset]!![table]!!.query

        val queryYesterday = query.addYesterdayRestriction()

        var success = true
        var yesterday = 0
        var total = 0

        val responseTotal = doSFQuery("${AccessTokenHandler.instanceUrl}${application.sfQueryBase}${query.addLimitRestriction()}")
        File("/tmp/responseAtTotalCall").writeText(responseTotal.toMessage())
        if (responseTotal.status.code == 400) {
            result += "Bad request: " + responseTotal.bodyString()
            File("/tmp/badRequestAtTotalCall").writeText(responseTotal.bodyString())
            success = false
        } else {
            try {
                val obj = JsonParser.parseString(responseTotal.bodyString()) as JsonObject
                val totalSize = obj["totalSize"].asInt
                Metrics.latestTotalFromTestCall.labels(table).set(totalSize.toDouble())
                result = "Total number of records found is $totalSize"
                log.info { "Total number of records found is $totalSize" }
                total = totalSize
            } catch (e: Exception) {
                success = false
                result += e.message
                File("/tmp/exceptionAtTotalCall").writeText(e.toString() + "\n" + e.stackTraceToString())
            }
        }
        val responseDate = doSFQuery("${AccessTokenHandler.instanceUrl}${application.sfQueryBase}${queryYesterday.addLimitRestriction()}")
        File("/tmp/responseAtDateCall").writeText(responseDate.toMessage())
        if (responseTotal.status.code == 400) {
            result += "\nBad request: " + responseDate.bodyString()
            File("/tmp/badRequestAtDateCall").writeText(responseDate.bodyString())
            success = false
        } else {
            try {
                val obj = JsonParser.parseString(responseDate.bodyString()) as JsonObject
                val totalSize = obj["totalSize"].asInt
                result += "\nNumber of records from yesterday poll $totalSize"
                log.info { "Number of records from yesterday poll $totalSize" }
                yesterday = totalSize
            } catch (e: Exception) {
                success = false
                result += "\n" + e.message
                File("/tmp/exceptionAtDateCall").writeText(e.toString() + "\n" + e.stackTraceToString())
            }
        }
        File("/tmp/testcallResult").writeText(result)

        val response = if (success) {
            val pair = Pair(yesterday, total)
            Response(Status.OK).body(gson.toJson(pair))
        } else {
            Response(Status.BAD_REQUEST).body(result)
        }

        File("/tmp/lasttestcallresponse").writeText(response.toMessage())
        response
    }

    // Data classes for metadata
    data class ColumnMetadata(
        val name: String,
        val type: String,
        val mode: String,
        val salesforceFieldName: String? = null
    )

    data class TableMetadata(
        val tableName: String,
        val numRows: Long,
        val columns: List<ColumnMetadata>,
        val salesforceQuery: String? = null,
        val active: Boolean = true,
        val operationInfo: OperationInfo
    )

    data class DatasetMetadata(
        val tables: List<TableMetadata>
    )

    // Function to fetch BigQuery metadata
    fun fetchBigQueryMetadata(mapDef: Map<String, Map<String, TableDef>>): BigQueryMetadata {
        val result = mutableMapOf<String, DatasetMetadata>()

        // List datasets in the project
        val datasets = application.bigQueryService.listDatasets(application.projectId).iterateAll()
        for (dataset in datasets) {
            val datasetName = dataset.datasetId.dataset

            // List of table metadata
            val tablesInfo = mutableListOf<TableMetadata>()

            // List tables in the dataset
            val tables = application.bigQueryService.listTables(datasetName).iterateAll()
            for (table in tables) {
                val tableName = table.tableId.table

                val tableQuery = mapDef[datasetName]?.get(tableName)?.query?.let { it.replace("+", " ") } ?: "No query configured"

                val selectFields = extractFields(tableQuery)
                // Get table metadata
                val fullTable: Table
                try {
                    // log.info { "Attempting to fetch table $tableName" }
                    fullTable = application.bigQueryService.getTable(table.tableId, BigQuery.TableOption.fields(BigQuery.TableField.NUM_ROWS, BigQuery.TableField.SCHEMA))
                    // log.info { "Found table $tableName - ${fullTable.numRows.toLong()}" }
                } catch (e: Exception) {
                    log.error { e.printStackTrace() }
                    throw e
                }
                // log.info { "Table definition before" }
                val definition = fullTable.getDefinition<StandardTableDefinition>()
                // log.info { "Table definition after - $tableName" }
                val numRows = fullTable.numRows.toLong()
                // log.info { "Table definition after numRows - $tableName" }
                val schema = definition.schema
                // log.info { "Table definition schema - $tableName - schema: $schema" }
                // List of column metadata
                val columns = mutableListOf<ColumnMetadata>()

                val fieldDefMap = mapDef[datasetName]?.get(tableName)?.fieldDefMap

                // log.info { "I got a fieldDefMap" }

                // Looking up BigQfields against mappings
                for (field in schema!!.fields) {
                    // log.info { "Looking up for bq $field - $tableName" }
                    // Lookup Salesforce field name from mapDef
                    val salesforceFieldName = fieldDefMap
                        ?.entries
                        ?.find { it.value.name == field.name } // Match BigQuery column name with fieldDefMap.name
                        ?.key?.let { if (selectFields.contains(it)) it else "$it - Missing in query" } ?: "No mapping configured"

                    val typeText = fieldDefMap
                        ?.entries
                        ?.find { it.value.name == field.name } // Match BigQuery column name with fieldDefMap.name
                        ?.value?.type?.name?.let { if (it == field.type.name()) it else "$it (configured) / ${field.type.name()} (Big Query) - Mismatch types" } ?: "No mapping configured"

                    // log.info { "Looking up $salesforceFieldName for $field for $tableName" }
                    val columnInfo = ColumnMetadata(
                        name = field.name,
                        type = typeText,
                        mode = field.mode?.name ?: "NULLABLE",
                        salesforceFieldName = salesforceFieldName // Populate with Salesforce field name
                    )
                    columns.add(columnInfo)
                }

                // Looking up SF fields in SELECT that has no mapping - therfor no definition what corresponding BigQ name is
                selectFields.filter { selectField -> fieldDefMap?.keys?.let { !it.contains(selectField) } ?: false }.forEach {
                    queryFieldUnmapped ->
                    val columnInfoQueryFieldNotMapped = ColumnMetadata(
                        name = "",
                        type = "",
                        mode = "",
                        salesforceFieldName = "$queryFieldUnmapped - No mapping configured" // Populate with Salesforce field name
                    )
                    columns.add(columnInfoQueryFieldNotMapped)
                }

                selectFields.filter { selectField -> fieldDefMap?.keys?.let { it.contains(selectField) } ?: false } // Has map entry
                    .filter { selectField ->
                        fieldDefMap?.entries?.find { it.key == selectField }?.value?.name.let { mappedBigQField ->
                            schema.fields.let { !(it.any { it.name == mappedBigQField }) } // mapped entry can not be found in big q schema
                        }
                    }.forEach {
                        queryFieldMappedToNonExistingBigQueryField ->
                        val columnInfoQueryFieldMappedToNonExistingBigQueryField = ColumnMetadata(
                            name = fieldDefMap?.entries?.find { it.key == queryFieldMappedToNonExistingBigQueryField }?.value?.name!! + " - Not existing",
                            type = "",
                            mode = "",
                            salesforceFieldName = queryFieldMappedToNonExistingBigQueryField // Populate with Salesforce field name
                        )
                        columns.add(columnInfoQueryFieldMappedToNonExistingBigQueryField)
                    }

                // Add table metadata to the list
                val tableMetadata = TableMetadata(
                    tableName = tableName,
                    numRows = numRows,
                    columns = columns,
                    salesforceQuery = tableQuery,
                    active = application.postToBigQuery && !(application.excludeTables.any { it == tableName }),
                    operationInfo = BulkOperation.operationInfo[datasetName]!![tableName]!!
                )
                tablesInfo.add(tableMetadata)
            }

            // Add dataset metadata to the result map
            val datasetMetadata = DatasetMetadata(tables = tablesInfo)
            result[datasetName] = datasetMetadata
        }

        return result
    }

    fun extractFields(query: String): List<String> {
        val regex = Regex("SELECT\\s+(.*?)\\s+FROM", RegexOption.IGNORE_CASE)
        val matchResult = regex.find(query)
        return matchResult?.groups?.get(1)?.value
            ?.split(",")
            ?.map { it.trim() } // Trim any extra spaces
            ?: emptyList()
    }
}
