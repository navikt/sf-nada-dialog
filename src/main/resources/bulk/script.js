window.onload = function() {
    console.log("Do on load");
    addConfigDescriptionAction();
    populateAction();

    checkActiveId()
};

function checkActiveId() {
    fetch('/internal/activeId')
        .then(response => response.text())  // Use .text() to handle plain text response
        .then(data => {
            if (data.trim()) {  // If there's a non-empty ID in the response
                const activeId = data.trim(); // Extract the ID from the response
                document.getElementById('status').innerHTML = 'Active ID found: ' + activeId;

                hideSelectElements()
                performBulk();
            } else {
                document.getElementById('status').innerHTML = 'No active ID found';
            }
        })
        .catch(error => {
            console.error('Error fetching active ID:', error);
        });
}

function performBulk() {
    fetch('/internal/performBulk')
        .then(response => response.json())
        .then(data => {
            // Display the response in the status element
            document.getElementById('status').innerHTML = 'Performing bulk action: ' + JSON.stringify(data);

            console.log('performBulk response:', data);
            console.log('performBulk stringify:', JSON.stringify(data));
            // Check the state of the bulk job every 3 seconds
            if (data.state && data.state === 'JobComplete') {
                showCompletionMessage(data)
            } else {
                document.getElementById('status').innerHTML = 'Status: '+ data.state + ', records processed ' + data.numberRecordsProcessed + ', job id:' + data.id;
                setTimeout(() => performBulk(), 3000); // Retry after 3 seconds
            }
        })
        .catch(error => {
            console.error('Error performing bulk action:', error);
        });
}

const populateAction = async () => {
    const response = await fetch('/internal/datasets', {
        method: 'GET',
        headers: {
            'Content-Type': 'application/json'
        }
    });

    var j = await response.json();

    let datasetDrop = document.getElementById("datasetDropdown");

    for (let i = 0; i < j.length; i++) {
        datasetDrop.innerHTML += '<a onclick="datasetSelect(\''+j[i]+'\')">'+j[i]+'</a>';
    }
}
function datasetDropdownBtnClick() {
    document.getElementById("datasetDropdown").classList.toggle("show");
}
function datasetSelect(name) {
    document.getElementById("datasetDropdownBtn").innerHTML = name;
    document.getElementById("tableDropdownBtn").innerHTML = "Table"
    document.getElementById("report").innerHTML = ""
    document.getElementById("bulkStartBtn").classList.remove("show")

    populateTableAction(name)
}

const populateTableAction = async ( dataset ) => {
    const response = await fetch('/internal/tables?dataset='+dataset, {
        method: 'GET',
        headers: {
            'Content-Type': 'application/json'
        }
    });

    var j = await response.json();

    let tableDrop = document.getElementById("tableDropdown");

    for (let i = 0; i < j.length; i++) {
        if (i == 0) {
            tableDrop.innerHTML = '<a>Table</a>';
        }
        tableDrop.innerHTML += '<a onclick="tableSelect(\''+dataset+'\',\''+j[i]+'\')">'+j[i]+'</a>';
    }
}

const populateSchemaMapAction = async ( dataset, table ) => {
    const response = await fetch('/internal/schemamap?dataset='+dataset+'&table='+table, {
        method: 'GET',
        headers: {
            'Content-Type': 'text/html'
        }
    });

    var t = await response.text();

    let reportdst = document.getElementById("report");

    reportdst.innerHTML = t.replaceAll("\n","<br>") + '<br><br><br><a class="dropbtn" onclick="runTest(\''+dataset+'\',\''+table+'\')">Perform query and count</a>';
}

const populateTestResultAction = async ( dataset, table ) => {
    let reportdst = document.getElementById("testreport");
    reportdst.innerHTML = "<br>Pending..."
    const response = await fetch('/internal/testcall?dataset='+dataset+'&table='+table, {
        method: 'GET',
        headers: {
            'Content-Type': 'text/html'
        }
    });

    var t = await response.text();

    reportdst.innerHTML = "<br>" + t.replaceAll("\n","<br>")
}

const addConfigDescriptionAction = async ( ) => {
    const response = await fetch('/internal/htmlforconfig', {
        method: 'GET',
        headers: {
            'Content-Type': 'text/html'
        }
    });

    var h = await response.text();

    let dst = document.body

    dst.innerHTML = dst.innerHTML + h
}

function tableDropdownBtnClick() {
    document.getElementById("tableDropdown").classList.toggle("show");
}
let selectedForBulkDataset;

let selectedForBulkTable;

function tableSelect(dataset, table) {
    document.getElementById("tableDropdownBtn").innerHTML = table;
    document.getElementById("bulkStartBtn").classList.add("show")
    selectedForBulkDataset = dataset;
    selectedForBulkTable = table;
    populateSchemaMapAction(dataset, table)
}
function runTest(dataset, table) {
    populateTestResultAction(dataset, table)
}

selectedForBulkDataset = '';
selectedForBulkTable = '';

async function bulkStartBtnClick() {
    const userConfirmed = confirm(`This will start batch job for ${selectedForBulkDataset} ${selectedForBulkTable}?`);
    if (userConfirmed) {
        const response = await fetch('/internal/performBulk?dataset=' + selectedForBulkDataset + '&table=' + selectedForBulkTable, {
            method: 'GET',
            headers: {
                'Content-Type': 'text/html'
            }
        });

        const t = await response.text()
        //alert(t)
        checkActiveId()
    }
}

// Close the dropdown if the user clicks outside of it
window.onclick = function(event) {
    if (!event.target.matches('.dropbtn')) {
        var dropdowns = document.getElementsByClassName("dropdown-content");
        var i;
        for (i = 0; i < dropdowns.length; i++) {
            var openDropdown = dropdowns[i];
            if (openDropdown.classList.contains('show')) {
                openDropdown.classList.remove('show');
            }
        }
    }
}

async function reconnectBtnClick() {
    const jobId = document.getElementById('activeJobId').value;
    if (jobId) {
        //alert(`Reconnecting to job ID: ${jobId}`);
        const response = await fetch('/internal/reconnect?id=' + jobId, {
            method: 'GET',
            headers: {
                'Content-Type': 'text/html'
            }
        });

        const t = await response.text()
        //alert(t)
        checkActiveId()
        // Add logic here to handle the reconnect action based on jobId
    } else {
        alert('Please enter a job ID to reconnect.');
    }
}

function hideSelectElements() {
    // Select elements by class name and hide them
    const elementsToHide = document.querySelectorAll('.jobIdHolder, .datasetDropdownHolder, .tableDropdownHolder, .bulkStartHolder, #description');
    elementsToHide.forEach(element => {
        element.style.display = 'none';
    });
}

function showCompletionMessage(data) {
    // Display completion message with records processed
    document.getElementById('status').innerHTML = 'Job Complete, records processed: ' + data.numberRecordsProcessed + ', job id:' + data.id

    // Add event listener for the button
    document.getElementById('startTransfer').addEventListener('click', function() {
        const confirmTransfer = confirm(`This will start bulk transfer of ${data.numberRecordsProcessed} records?`);
        if (confirmTransfer) {
            // Call the function to start bulk transfer here
            startBulkTransfer(data.numberRecordsProcessed);
            document.getElementById('startTransfer').classList.remove("show")
        }
    });

    document.getElementById('startTransfer').classList.add("show")
}

function startBulkTransfer(numberRecords) {
    document.getElementById('status').innerHTML = `Triggered data transfer of ${numberRecords} records`
    fetchTransferResults()
}

function fetchTransferResults() {
    fetch('/internal/transfer')
        .then(response => response.text())
        .then(data => {
            // Display the response in the status element
            document.getElementById('status').innerHTML = 'Results: ' + data;
        })
        .catch(error => {
            console.error('Error performing bulk action:', error);
        });
}