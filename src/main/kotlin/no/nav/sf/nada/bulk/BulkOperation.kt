package no.nav.sf.nada.bulk

object BulkOperation {
    @Volatile
    var operationIsActive: Boolean = false

    var dataset: String = ""

    var table: String = ""

    var jobId: String = ""
}

/**
 *
 * POST /services/data/v57.0/jobs/query
Content-Type: application/json
Authorization: Bearer <access_token>

{
"operation": "query",
"query": "SELECT Id, TAG_ActivityType__c, TAG_service__c, Subject, CreatedDate, LastModifiedDate, CRM_AccountOrgNumber__c, ActivityDate, TAG_AccountNAVUnit__c, TAG_AccountOrgType__c, TAG_UserNAVUnit__c, TAG_AccountParentId__c, TAG_AccountParentOrgNumber__c, TAG_Status__c, Type, CRM_Region__c, CRM_Unit__c, ReminderDateTime, RecordTypeId, StartDateTime, EndDateTime, EventSubtype, DurationInMinutes, WhatId, WhoId, TAG_IACaseNumber__c, IASubtheme__c, TAG_IACooperationId__c, RecordTypeName__c FROM Event",
"contentType": "CSV"
}
 */
