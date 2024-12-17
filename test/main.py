from fastapi import FastAPI

app = FastAPI()


@app.get("/")
async def root():
    print('hello')
    x = -5
    y = x if x > 0 else 0
    print(y)
    return {"message": "Hello World"}