package no.nav.sf.nada.bulk

import com.google.cloud.bigquery.TableId
import com.google.gson.JsonObject
import com.google.gson.JsonParser
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.launch
import mu.KotlinLogging
import no.nav.sf.nada.Bootstrap
import no.nav.sf.nada.doSFBulkJobResultQuery
import no.nav.sf.nada.doSFBulkJobStatusQuery
import no.nav.sf.nada.doSFBulkStartQuery
import no.nav.sf.nada.parseCSVToJsonArray
import no.nav.sf.nada.remapAndSendRecords
import org.http4k.core.HttpHandler
import org.http4k.core.Response
import org.http4k.core.Status
import java.io.File
import java.lang.Exception
import java.lang.RuntimeException

object BulkOperation {
    private val log = KotlinLogging.logger { }

    @Volatile
    var operationIsActive: Boolean = false

    var dataset: String = ""

    var table: String = ""

    var jobId: String = ""

    @Volatile
    var jobComplete: Boolean = false

    var currentResultLocator: String? = null

    @Volatile
    var dataTransferIsActive: Boolean = false

    @Volatile
    var dataTransferReport: String = ""

    @Volatile
    var transferDone: Boolean = false

    var processedRecords = 0

    var missingFieldWarning = 0

    val missingFieldNames: MutableSet<String> = mutableSetOf()

    val performBulkHandler: HttpHandler = {
        if (!operationIsActive) {
            val dataset = it.query("dataset")
            val table = it.query("table")
            BulkOperation.dataset = dataset!!
            BulkOperation.table = table!!
            val bulkResponse = doSFBulkStartQuery(BulkOperation.dataset, BulkOperation.table)
            try {
                File("/tmp/bulkstartQueryResponse").writeText(bulkResponse.toMessage())
                val responseObj = JsonParser.parseString(bulkResponse.bodyString()) as JsonObject
                jobId = responseObj["id"].asString
                operationIsActive = true
            } catch (e: Exception) {
                log.error { e.stackTraceToString() }
            }
        }
        val bulkJobStatusResponse = doSFBulkJobStatusQuery(jobId)
        try {
            val responseObj = JsonParser.parseString(bulkJobStatusResponse.bodyString()) as JsonObject
            jobId = responseObj["id"].asString
            operationIsActive = true
            if (responseObj["state"].asString == "JobComplete") {
                File("/tmp/jobRegisteredAsDoneByPerformBulk").writeText("yes")
                jobComplete = true
            }
        } catch (e: Exception) {
            log.error { e.stackTraceToString() }
        }
        Response(Status.OK).body(bulkJobStatusResponse.bodyString())
    }

    val reconnectHandler: HttpHandler = {
        val id = it.query("id")
        val dataset = it.query("dataset")
        val table = it.query("table")
        jobId = id!!
        BulkOperation.dataset = dataset!!
        BulkOperation.table = table!!
        operationIsActive = true
        log.info { "Reconnecting gui to jobId $id" }
        Response(Status.OK).body("Reconnected to jobId $id, dataset $dataset, table $table")
    }

    val activeIdHandler: HttpHandler = {
        Response(Status.OK).body(if (BulkOperation.operationIsActive) "${BulkOperation.jobId} (${BulkOperation.dataset}, ${BulkOperation.table})" else "")
    }

    val transferHandler: HttpHandler = {
        if (!operationIsActive) {
            Response(Status.PRECONDITION_FAILED).body("No batch operation initiated")
        } else if (!jobComplete) {
            // Check if job is done since last check
            val bulkJobStatusResponse = doSFBulkJobStatusQuery(jobId)
            val responseObj = JsonParser.parseString(bulkJobStatusResponse.bodyString()) as JsonObject
            if (responseObj["state"].asString == "JobComplete") {
                jobComplete = true
            }
        }
        if (!jobComplete) {
            Response(Status.PRECONDITION_FAILED).body("Batch job is not finished - cannot do data transfer yet")
        } else if (transferDone) {
            Response(Status.OK).body(dataTransferReport)
        } else if (dataTransferIsActive) {
            Response(Status.ACCEPTED).body(dataTransferReport)
        } else {
            dataTransferIsActive = true
            GlobalScope.launch {
                runTransferJob()
            }
            dataTransferReport = "Job $jobId started${if (dataset.isNotEmpty()) " for $dataset, $table" else ""}...${if (!Bootstrap.postToBigQuery) " (Will not actually post due to postToBigQuery flag false)" else ""}"
            Response(Status.ACCEPTED).body(dataTransferReport)
        }
    }

    fun runTransferJob() {
        log.info { "Starting bulk transfer from batch job $jobId to $dataset $table" }
        val fieldDef = Bootstrap.mapDef[dataset]!![table]!!.fieldDefMap
        val tableId = TableId.of(dataset, table)
        var locator: String? = null

        do {
            val response = doSFBulkJobResultQuery(jobId, locator)
            val array = parseCSVToJsonArray(response.bodyString())
            try {
                remapAndSendRecords(array, tableId, fieldDef)
            } catch (e: Exception) {
                dataTransferReport += "\nFail in batch operation - ${e.message}"
                transferDone = true
                throw RuntimeException("Fail in batch operation - ${e.message}")
            }

            // Next locator
            locator = response.header("Sforce-Locator")
            if (locator == "null") locator = null
            processedRecords += array.size()
            val reportRow = "Processed ${array.size()} records, next locator: $locator"
            log.info { reportRow }
            dataTransferReport += "\n$reportRow"
            currentResultLocator = locator
            if (locator == null) {
                dataTransferReport += "\nDone ($processedRecords records processed)"
                log.info { "Done ($processedRecords records processed)" }
                if (missingFieldWarning > 0) {
                    dataTransferReport += "\nWarning: Expected fields $missingFieldNames missing in record, total sum ($missingFieldWarning)"
                    log.info { "Warning: Expected fields $missingFieldNames missing in record, total sum ($missingFieldWarning)" }
                }
                transferDone = true
            }
        } while (locator != null)
    }
}
