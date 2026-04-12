import asyncio
import os

# simulating sse behavior over raw TCP connection
async def handle_tcp_connection(clientWsReader: asyncio.StreamReader, clientWsWriter: asyncio.StreamWriter):
    print("New TCP connection established")
    try:
        request_line = await clientWsReader.readline()
        request_str = request_line.decode('utf-8')
        # very first request
        # client sending the request to server to get the index.html file and app.js file
        if request_str.startswith("GET / HTTP/"):
            print("serve index.html")
            with open("index.html", "r") as f:
                content = f.read()
            response = (
                "HTTP/1.1 200 OK\r\n"
                "Content-Type: text/html\r\n"
                f"Content-Length: {len(content)}\r\n"
                "\r\n"
                f"{content}"
            )
            clientWsWriter.write(response.encode('utf-8'))
            await clientWsWriter.drain()
        # after sending index.html
        # index.html will send a request to server to get app.js file

        elif request_str.startswith("GET /app.js HTTP/"):
            print("serve app.js")
            with open("app.js", "r") as f:
                content = f.read()
            response = (
                "HTTP/1.1 200 OK\r\n"
                "Content-Type: application/javascript\r\n"
                f"Content-Length: {len(content)}\r\n"
                "\r\n"
                f"{content}"
            )
            clientWsWriter.write(response.encode('utf-8'))
            await clientWsWriter.drain()
        # logic in app.js will send a request to server to get sse stream
        elif request_str.startswith("GET /sse HTTP/"):
            print("serve sse stream")
            # send content-type as text/event-stream to client
            response=(
                "HTTP/1.1 200 OK\r\n"
                "Content-Type: text/event-stream\r\n"
                "Cache-Control: no-cache\r\n"
                "Connection: keep-alive\r\n"
                "\r\n"
            )
            clientWsWriter.write(response.encode())
            await clientWsWriter.drain()

            # i will send 5 events every 2 seconds
            for i in range(5):
                event_data = f"data: Event {i}\n\n"
                clientWsWriter.write(event_data.encode('utf-8'))
                await clientWsWriter.drain()
                await asyncio.sleep(2)
            print("Closing TCP connection")

            termination_data = "event: end\ndata: Stream ended\n\n"
            clientWsWriter.write(termination_data.encode('utf-8'))
            await clientWsWriter.drain()
        else:
            print(f"Unknown request: {request_str}")
            clientWsWriter.write(b"HTTP/1.1 404 Not Found\r\n\r\n")
            await clientWsWriter.drain()
    except Exception as e:
        print(f"Error handling TCP connection: {e}")
    finally:
        print("TCP connection closed")
        clientWsWriter.close()

async def main():
    server = await asyncio.start_server(handle_tcp_connection, '127.0.0.1', 8000)
    print("Server running on http://127.0.0.1:8000")
    async with server:
        await server.serve_forever()

if __name__ == '__main__':
    asyncio.run(main())