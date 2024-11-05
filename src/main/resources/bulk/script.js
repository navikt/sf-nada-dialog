window.onload = function() {
    console.log("Do on load");
    addConfigDescriptionAction();
    populateAction();
};

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
    document.getElementById("bulkStartBtn").classList.toggle("show")
    selectedForBulkDataset = dataset;
    selectedForBulkTable = table;
    populateSchemaMapAction(dataset, table)
}
function runTest(dataset, table) {
    populateTestResultAction(dataset, table)
}

selectedForBulkDataset = '';
selectedForBulkTable = '';

function bulkStartBtnClick() {
    const userConfirmed = confirm(`Will do batch on ${selectedForBulkDataset} ${selectedForBulkTable}`);
    const response = fetch('/internal/performBulk?dataset='+selectedForBulkDataset+'&table='+selectedForBulkTable, {
        method: 'GET',
        headers: {
            'Content-Type': 'text/html'
        }
    });

    var t = response.text()
    alert(t + " userconfirmed" + userConfirmed)
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

function reconnectBtnClick() {
    const jobId = document.getElementById('activeJobId').value;
    if (jobId) {
        alert(`Reconnecting to job ID: ${jobId}`);
        // Add logic here to handle the reconnect action based on jobId
    } else {
        alert('Please enter a job ID to reconnect.');
    }
}