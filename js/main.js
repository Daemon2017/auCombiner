function getLink() {
    document.getElementById("statusID").value = "";
    var xhr = new XMLHttpRequest();
    var uuid = crypto.randomUUID();

    xhr.onreadystatechange = function () {
        if (xhr.readyState == 4 && xhr.status == 200) {
            document.getElementById("statusID").value = "Everything is prepared!  Waiting...";
            sendFile(xhr.response, uuid);
        }
    }
    xhr.responseType = "text";

    xhr.open("GET", "https://bba7i9qihm3snnrvme3r.containers.yandexcloud.net/get_link");
    xhr.setRequestHeader("key", uuid);

    xhr.send();
}

function sendFile(link, uuid) {
    var xhr = new XMLHttpRequest();

    xhr.onreadystatechange = function () {
        if (xhr.readyState == 4 && xhr.status == 200) {
            document.getElementById("statusID").value = "File uploaded!  Waiting...";
            combine(uuid);
        }
    }
    xhr.responseType = "text";

    xhr.open("PUT", link);

    var formData = new FormData();
    formData.append("file", document.getElementById("fileID").files[0]);

    xhr.send(formData);
}

function combine(uuid) {
    var xhr = new XMLHttpRequest();

    xhr.onreadystatechange = function () {
        if (xhr.readyState == 4 && xhr.status == 200) {
            document.getElementById("statusID").value = "Processed! Waiting...";
            getFile(xhr.response);
        }
    }
    xhr.responseType = "text";

    xhr.open("POST", "https://bba7i9qihm3snnrvme3r.containers.yandexcloud.net/combine");
    xhr.setRequestHeader("key", uuid);

    xhr.send();
}

function getFile(link) {
    var xhr = new XMLHttpRequest();

    xhr.onreadystatechange = function () {
        if (xhr.readyState == 4 && xhr.status == 200) {
            document.getElementById("statusID").value = "Success!";
            var blob = new Blob([xhr.response], { type: "octet/stream" });
            saveAs(blob, "combined.csv");
        }
    }
    xhr.responseType = "arraybuffer";

    xhr.open("GET", link);

    xhr.send();
}