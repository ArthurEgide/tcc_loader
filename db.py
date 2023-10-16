import sqlalchemy as db
from sqlalchemy import create_engine, URL, text
from sqlalchemy import *
import pymysql

MAX_SIZE = 5000
BATCH_SIZE = 500

def check_apostrophe(s):
  try:
    int(s)
    return str(s)
  except ValueError:
    if("'" in s):
      s = s.replace("'", "\\'")
      return f"E'{s}'"
    return f"'{s}'"

def get_conn():
  url_object = URL.create(
      'postgresql',
      username='arthur',
      password='egide',
      host='postgres',
      database='postgres',
  )
  engine = create_engine(url_object)
  connection = engine.connect()
  return connection

def insert_cards_data(data):
    conn = get_conn()
    card_ids = {
        'min': 0,
        'max': 0
    }

    # Preparar a declaração SQL com placeholders
    insert_statement = text("""
        INSERT INTO public.cards(nome, tipo, custo_mana, descricao, poder, resistencia, raridade)
        VALUES (:nome, :tipo, :custo_mana, :descricao, :poder, :resistencia, :raridade)
    """)
    
    data = data[:MAX_SIZE]
    data_batches = [data[i:i+BATCH_SIZE] for i in range(0, len(data), BATCH_SIZE)]

    for batch in data_batches:
      conn.execute(insert_statement, batch)

    card_ids['max'] = conn.execute(text("SELECT MAX(card_id) FROM public.cards")).scalar()
    card_ids['min'] = card_ids['max'] - len(data)

    conn.commit()
    conn.close()
    return card_ids

def insert_decks_data(data):
  deck_ids = {
    'min': 0,
    'max': 0
  }
  data = data[:MAX_SIZE]
  data_batches = [data[i:i+BATCH_SIZE] for i in range(0, len(data), BATCH_SIZE)]
  conn = get_conn()

  insert_statement = text("""
        INSERT INTO public.decks(nome, descricao, formato, numero_cartas)
        VALUES (:nome, :descricao, :formato, :numero_cartas)
    """)

  for batch in data_batches:
    conn.execute(insert_statement, batch)
  
  deck_ids['max'] = conn.execute(text("SELECT MAX(deck_id) FROM public.decks")).scalar()
  deck_ids['min'] = deck_ids['max'] - len(data)
  
  conn.commit()
  conn.close()
  return deck_ids
    
def insert_deck_cards_data(data):
  try:
    conn = get_conn()
    insert_statement = text("""
        INSERT INTO public.deck_cards(deck_id, card_id, quantidade)
        VALUES (:deck_id, :card_id, :quantidade)
    """)
    # Deck size is 60 cards
    max_decks = MAX_SIZE // 60
    for deck in data[:max_decks]:
      conn.execute(insert_statement, deck)
    
    conn.commit()
    conn.close()
  except Exception as e:
    print(e)

def insert_performance_step(data):
  conn = get_conn()
  insert_statement = text(f"INSERT INTO public.steps( execucao, horario, acao, tabela, quantidade, id ) VALUES(:execucao, :horario, :acao, :tabela, :quantidade, :id)")
  conn.execute(insert_statement, data)
  conn.commit()
  conn.close()