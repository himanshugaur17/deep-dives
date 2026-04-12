const div = document.getElementById("log");

const eventSource = new EventSource("/sse");

eventSource.onmessage = (event) => {
    div.innerHTML += "Received: " + event.data + "<br>";
    div.scrollTop = div.scrollHeight;
}

eventSource.addEventListener("end", (event) => {
    div.innerHTML += "Received end event: " + event.data + "<br>";
    eventSource.close();
})