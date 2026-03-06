from fastapi import FastAPI
from api.routes import router
import os

app = FastAPI()

app.include_router(router)

@app.get("/")
def home():
    return {"message": "Bijlipay Recon API is running"}

port = int(os.environ.get("PORT", 8000))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=port)
