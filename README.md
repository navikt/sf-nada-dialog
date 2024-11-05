# sf-nada-dialog

This is the sf-nada instance for team-dialog

The sf-nada app is an integration between salesforce and the NADA data product solution (see https://docs.knada.io/)

Its default behaviour is to run a work session each morning (~02:00) where it fetches all data records from salesforce
last modified yesterday and post them to corresponding bigquery tables of the NADA solution.

The behaviour can be changed with config in [dev-gcp.yaml](.nais/dev-gcp.yaml) and [prod-gcp.yaml](.nais/prod-gcp.yaml)

The query and mapping for each data product is set up in the map definition files: [dev.json](src/main/resources/mapdef/dev.json)
and [prod.json](src/main/resources/mapdef/prod.json)

Each push to this repository will trigger a deployment to either dev-gcp or prod-gcp, defined in [main.yml](.github/workflows/main.yml)

You can examine the current state of the app at these ingresses (naisdevice required):

Dev: https://sf-nada-dialog.intern.dev.nav.no/examine

Prod: https://sf-nada-dialog.intern.nav.no/examine

### Config

You will see the current active config on the examine-ingresses above. They are set by env-variables in [dev-gcp.yaml](.nais/dev-gcp.yaml) and [prod-gcp.yaml](.nais/prod-gcp.yaml)
#### POST_TO_BIGQUERY
Default is true. Whether you will actually send the fetched data to bigquery or not.
#### RUN_SESSION_ON_STARTUP
Default is false. This will trigger a work session directly on deploy, instead of waiting to next morning. Use for instance when you want to do an initial data dump.
Remember to turn this to false once you are done since you otherwise will have duplicate posts each time the app reboots.
#### FETCH_ALL_RECORDS
Default is false. This will ignore the LastModifedDate == yesterday restriction when fetching data from salesforce and fetch all records found.
Used for instance when you want to do an initial data dump.
#### EXCLUDE_TABLES
Default is ''. A comma seperated list of tables to ignore when fetching data. Used for instance to ignore existing products when performing a data dump on a new one.

### Map definition

The query and mapping for each data product is setup in the map definition files [dev.json](src/main/resources/mapdef/dev.json)
and [prod.json](src/main/resources/mapdef/prod.json).
They are well formed json-objects defined as:
```
{
    "<dataset>": { 
        "<table>": {
            "query": "<query to run in salesforce to fetch all records for product>" 
            "schema": {
                "<field name in salesforce>": {
                    "name": "<column name in bigquery>",
                    "type": "<type in bigquery - one of STRING, INTEGER, DATETIME, DATE or BOOL>"
                 },
                ...
            }
        }
    }
}
```
Note that you can map nested fields in Salesforce. I.e "LiveChatButton.MasterLabel" is a legal field name.

### Examine-ingesses

You can use the examine ingresses (https://sf-nada-dialog.dev.intern.nav.no/internal/examine, https://sf-nada-dialog.intern.nav.no/internal/examine) to examine the state of the app in dev and prod. Here you can click on the dataset- and table buttons
to verify how the current deployed map definition file are being parsed by the app. You can also run the query for that table (returning a total count) to see if the fetch from salesforce
goes through successfully.

If you are deploying with intent to verify a new addition in the examine-ingress, it is recommended to turn off POST_TO_BIG_QUERY until you are happy with the verification.