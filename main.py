from sanic import Sanic
from sanic.response import text
from db import insert_data

app = Sanic("ArthurEgideTCC_PythonLoader")

@app.post("/create_cards")
async def create_cards(request):
    body = request.json
    insert_data(body)
    return text(f"Done\n")
