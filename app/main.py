import uvicorn
from fastapi import FastAPI

from app.api.views import home

app = FastAPI()
app.add_api_route('/', home)


if __name__ == "__main__":
    uvicorn.run("app.main:app",host='0.0.0.0', port=8080, reload=True, debug=True, workers=3)

