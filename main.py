from sanic import Sanic
from sanic.response import json
from db import insert_cards_data, insert_decks_data, insert_deck_cards_data, insert_performance_step
from urllib.parse import parse_qs
import json as j

app = Sanic("ArthurEgideTCC_PythonLoader")

@app.get("/")
async def health(request):
    print("Health paizão")
    return json(body={"data": "Health"}, status=200 )

@app.post("/register_step")
async def register_step(request):
    body = request.json
    r = insert_performance_step(body)
    return json(r)

@app.post("/create_cards")
async def create_cards(request):
  try:
    if(type(request.body) == bytes):
      try:
        body = request.body.decode('UTF-8')
      except UnicodeDecodeError as e:
          body = request.body.decode('latin1')
      data = j.loads(body)
    else:
      data = request.json
    r = insert_cards_data(data)
    return json(r)
  except Exception as e:
    print(e)

@app.post("/create_decks")
async def create_decks(request):
    body = request.json
    r = insert_decks_data(body)
    return json(r)

@app.post("/create_deck_cards")
async def create_deck_cards(request):
    body = request.json
    insert_deck_cards_data(body)
    return json({
        "statusMessage": "OK"
    })
