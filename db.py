import sqlalchemy as db
from sqlalchemy import create_engine, URL, text
from sqlalchemy import *
import pymysql

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
        RETURNING card_id
    """)

    batch_size = 1000
    data_batches = [data[i:i+batch_size] for i in range(0, len(data), batch_size)]

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
  conn = get_conn()
  for d in data:
    nome = d.get("nome")
    descricao = d.get("descricao")
    formato = d.get("formato")
    numero_cartas = d.get("numero_cartas")

    columns = [ nome, descricao, formato, numero_cartas ]
    
    row = ",".join([ check_apostrophe(valor) if type(valor) == str else str(valor) for valor in columns ])
    statement = text(f"INSERT INTO public.decks(nome, descricao, formato, numero_cartas) VALUES({row}) returning deck_id")
    x = conn.execute(statement)
  
  deck_ids['max'] = x.fetchone()[0]
  deck_ids['min'] = deck_ids['max'] - len(data)
  conn.commit()
  conn.close()
  return deck_ids
    
def insert_deck_cards_data(data):
  conn = get_conn()
  for deck in data:
    for card in deck:
      deck_id = card.get("deck_id")
      card_id = card.get("card_id")
      quantidade = card.get("quantidade")
      
      columns = [ deck_id, card_id, quantidade ]
      
      row = ",".join([ check_apostrophe(valor) for valor in columns ])
      statement = text(f"INSERT INTO public.deck_cards(deck_id, card_id, quantidade) VALUES({row})")
      conn.execute(statement)
  
  conn.commit()
  conn.close()

def insert_performance_step(data):
  conn = get_conn()
  execucao = data.get("execucao")
  horario = data.get("horario")
  acao = data.get("acao")
  tabela = data.get("tabela")
  quantidade = data.get("quantidade")
  id = data.get("id")
  
  columns = [ execucao, horario, acao, tabela, quantidade, id ]
  row = ",".join([ check_apostrophe(valor) for valor in columns ])
  statement = text(f"INSERT INTO public.steps( execucao, horario, acao, tabela, quantidade, id ) VALUES({row})")
  conn.execute(statement)
  conn.commit()
  conn.close()