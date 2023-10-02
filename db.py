import sqlalchemy as db
from sqlalchemy import create_engine, URL, text
from sqlalchemy import *
import pymysql

def check_apostrophe(s):
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
  
def insert_data(data):
  conn = get_conn()
  for d in data:
    nome = d.get("nome")
    tipo = d.get("tipo")
    custo_mana = d.get("custo_mana")
    descricao = d.get("descricao")
    poder = d.get("poder")
    resistencia = d.get("resistencia")
    raridade = d.get("raridade")

    columns = [ nome, tipo, custo_mana, descricao, poder, resistencia, raridade ]

    row = ",".join([ check_apostrophe(valor) if type(valor) == str else str(valor) for valor in columns ])
    statement = text(f"INSERT INTO public.cards(nome, tipo, custo_mana, descricao, poder, resistencia, raridade) VALUES({row})")
    conn.execute(statement)
  
  conn.commit()
  conn.close()
