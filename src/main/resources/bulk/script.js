window.onload = function() {
    console.log("Do on load");
    addConfigDescriptionAction();
    populateAction();

    checkActiveId()
};

const statusElement = document.getElementById('status');

function checkActiveId() {
    fetch('/internal/activeId')
        .then(response => response.text())  // Use .text() to handle plain text response
        .then(data => {
            if (data.trim()) {  // If there's a non-empty ID in the response
                const activeId = data.trim(); // Extract the ID from the response
                statusElement.innerHTML = 'Active ID found: ' + activeId;
                performBulk();
            } else {
                statusElement.innerHTML = 'No active ID found';
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
            statusElement.innerHTML = 'Performing bulk action: ' + JSON.stringify(data);

            console.log('performBulk response:', data);
            console.log('performBulk stringify:', JSON.stringify(data);
            // Check the state of the bulk job every 3 seconds
            if (data.state && data.state === 'JobComplete') {
                statusElement.innerHTML = 'Job Complete: ' + JSON.stringify(data);
            } else {
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
        alert(t)
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
        alert(`Reconnecting to job ID: ${jobId}`);
        const response = await fetch('/internal/reconnect?id=' + jobId, {
            method: 'GET',
            headers: {
                'Content-Type': 'text/html'
            }
        });

        const t = await response.text()
        alert(t)
        // Add logic here to handle the reconnect action based on jobId
    } else {
        alert('Please enter a job ID to reconnect.');
    }
}