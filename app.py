import os
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(title="Bijlipay Reconciliation Engine", version="1.0.0")

app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

@app.get("/health")
def health():
    return {"status": "ok", "service": "bijlipay-recon-engine"}

@app.get("/")
def root():
    return {"service": "Bijlipay Reconciliation Engine", "version": "1.0.0", "docs": "/docs"}
