import uvicorn
from fastapi import FastAPI

app = FastAPI()


@app.get("/api/v1/health")
def health():
    return {"status": "ok"}


if __name__ == "__main__":
    uvicorn.run(app, port=8000, host="0.0.0.0")
