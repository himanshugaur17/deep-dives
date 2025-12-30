import asyncio
from fastapi import FastAPI, BackgroundTasks, Request
from fastapi.responses import JSONResponse, StreamingResponse, HTMLResponse
from fastapi.templating import Jinja2Templates
import redis.asyncio as redis
import uuid
import json
import random

from worker import run_build
app=FastAPI()
r=redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
templates = Jinja2Templates(directory="templates")

@app.get("/", response_class=HTMLResponse)
async def get_page(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

@app.post("/jobs/start")
async def start_job(background_tasks: BackgroundTasks):
    job_id=str(uuid.uuid4())[:8]  # short unique id
    background_tasks.add_task(run_build, job_id)
    return JSONResponse(content={"job_id": job_id, "status": "started"})

@app.get("/jobs/{job_id}/history")
async def get_job_history(job_id: str):
    list_key=f"logs:{job_id}"
    logs = await r.lrange(list_key, 0, -1)
    log_messages = [json.loads(log) for log in logs]
    return JSONResponse(content={"logs":log_messages})

@app.get("/stream/{job_id}")
async def stream_logs(job_id:str, last_id: int = 0):
    async def event_generator():
        channel = f"channel:{job_id}"
        sleep_time=random.uniform(4,8)
        print(f"Subscribing to channel {channel} after sleeping for {sleep_time} seconds to simulate dely for pubsub to get ready")
        await asyncio.sleep(sleep_time) # delay to simulate time taken for pubsub to be ready
        pubsub = r.pubsub()
        await pubsub.subscribe(channel)

        list_key = f"logs:{job_id}"
        may_be_mised_logs = await r.lrange(list_key, last_id, -1)
        max_id=last_id
        for log in may_be_mised_logs:
            yield f"data: {log}\n\n"
            log_data=json.loads(log)
            max_id=log_data['id'] if log_data['id']>max_id else max_id
        
        try:
            async for message in pubsub.listen():
                if message['type'] == 'message':
                    data_str = message['data']
                    data = json.loads(data_str)
                    if data['id'] > max_id:
                        yield f"data: {data_str}\n\n"
                        max_id = data['id']
                    if data['id'] == data['total_steps']:
                        print(f"Build {job_id} completed. Exiting stream.")
                        break
        except asyncio.CancelledError:
            print("Client disconnected, stopping log stream.")
            await pubsub.unsubscribe(channel)
    return StreamingResponse(event_generator(), media_type="text/event-stream")



if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)


