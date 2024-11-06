package no.nav.sf.nada

import com.google.cloud.bigquery.BigQueryOptions
import mu.KotlinLogging
import no.nav.kafka.dialog.PrestopHook
import no.nav.kafka.dialog.ShutdownHook
import no.nav.kafka.dialog.enableNAISAPI
import org.http4k.client.ApacheClient
import java.time.LocalDate
import java.time.LocalTime
import java.time.format.DateTimeFormatter

private val resetRangeStart = LocalTime.parse("00:00:01")
private val resetRangeStop = LocalTime.parse("01:59:00")

private val log = KotlinLogging.logger { }

fun main() = Bootstrap.start()

object Bootstrap {

    val projectId = env(env_GCP_TEAM_PROJECT_ID)

    val SF_QUERY_BASE = env(env_SF_QUERY_BASE)

    val mapDef = parseMapDef(env(env_MAPDEF_FILE))

    val postToBigQuery = envAsBoolean(env_POST_TO_BIGQUERY)

    val runSessionOnStartup = envAsBoolean(env_RUN_SESSION_ON_STARTUP)

    val fetchAllRecords = envAsBoolean(env_FETCH_ALL_RECORDS)

    val excludeTables = envAsList(env_EXCLUDE_TABLES)

    var hasPostedToday = !runSessionOnStartup

    val client = lazy { ApacheClient() }

    val bigQueryService =
        BigQueryOptions.newBuilder()
            .setProjectId(projectId)
            .build().service

    fun start() {
        log.info { "Starting app with settings: projectId $projectId, postTobigQuery $postToBigQuery, runSessionOnStartup $runSessionOnStartup, fetchAllRecords $fetchAllRecords, excludeTables $excludeTables" }

        enableNAISAPI {
            log.info { "1 minutes graceful start - establishing connections" }
            Thread.sleep(60000)

            // One offs (remember to remove after one run):
            /*
            oneOff("2024-05-23")
            oneOff("2024-05-30")
            oneOff("2024-06-03")
            oneOff("2024-06-07")
            oneOff("2024-06-10")
            oneOff("2024-06-13")
             */
            //

            loop()
        }
        log.info { "App Finished!" }
    }

    fun oneOff(localDateAsString: String) = work(LocalDate.parse(localDateAsString))

    private tailrec fun loop() {

        val stop = ShutdownHook.isActive() || PrestopHook.isActive()
        when {
            stop -> Unit
            !stop -> {
                if (hasPostedToday) {
                    if (LocalTime.now().inResetRange()) {
                        log.warn { "It is now a new day - set posted flag back to false" }
                        hasPostedToday = false
                    } else {
                        // log.info { "Has posted logs today - will sleep 30 minutes." }
                    }
                } else {
                    if (LocalTime.now().inActiveRange()) {
                        work()
                    } else {
                        log.info { "Waiting for active range (later then ${resetRangeStop.format(DateTimeFormatter.ISO_DATE)}) - will sleep 30 minutes." }
                    }
                }
                conditionalWait(1800000) // Half an hour
                loop()
            }
        }
    }

    private fun LocalTime.inResetRange(): Boolean {
        return this.isAfter(resetRangeStart) && this.isBefore(resetRangeStop)
    }

    private fun LocalTime.inActiveRange(): Boolean {
        return this.isAfter(resetRangeStop)
    }
}
