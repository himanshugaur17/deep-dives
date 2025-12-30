from fastapi import FastAPI, HTTPException, Request, Response
import hmac
import time
import hashlib
from pydantic import BaseModel

app=FastAPI()
AWS_SECRET_KEY="my-secret"

class GenerateRequest(BaseModel):
    s3_path: str

def calculate_signature(path, expires_at):
    signing_string=f"{path}.{expires_at}"
    signature=hmac.new(AWS_SECRET_KEY.encode('utf-8')
                       ,signing_string.encode('utf-8')
                       ,hashlib.sha256).hexdigest()
    return signature

@app.post("/generate-presigned-url")
def generate_url(req:GenerateRequest):
    s3_path = req.s3_path
    expiry_timestamp=str(int(time.time())+120)
    signature=calculate_signature(s3_path, expiry_timestamp)

    return {"url":f"http://localhost:8000/{s3_path}?"
            f"expires={expiry_timestamp}&"
            f"signature={signature}"}

@app.post("/{bucket}/{filename}")
async def upload(bucket:str, filename:str, expires:str, signature:str):
    s3_path=f"{bucket}/{filename}"

    recalculated_signature=calculate_signature(s3_path,expires)
    if not hmac.compare_digest(recalculated_signature, signature):
        print(f"signature mismatch\n")
        raise HTTPException(status_code=403, detail="Access denied, mismatch in recalculated signature and signature sent in the req")
    
    current_time=int(time.time())

    if current_time > int(expires):
        print(f"Access denied, URL expired")
        raise HTTPException(status_code=403, detail="Access denied, URL expired")
    
    return {"message":"Upload successful"}

if __name__=="__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
