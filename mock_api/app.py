"""API mock de crédito (fonte do projeto de CDC).

Simula um sistema de origem que muda ao longo do tempo: clientes, contratos e parcelas.
Ela NÃO tem CDC nativo (como a maioria das APIs REST). O que ela oferece, e o que o
ingestor vai usar para derivar mudanças:
  - updated_at em toda linha (watermark incremental)
  - soft delete (is_deleted = 1, e updated_at muda)
  - hard delete (a linha some; o id fica em /deletions como "tombstone")
"""
import os
import random
import sqlite3
from collections import Counter
from contextlib import contextmanager
from datetime import date, datetime, timedelta, timezone

from fastapi import FastAPI, HTTPException, Query

DB_PATH = os.getenv("DB_PATH", "/data/mock.db")
ENTIDADES = {"clientes", "contratos", "parcelas"}  # allowlist: evita SQL injection no nome da tabela
CIDADES = ["Blumenau", "Joinville", "Florianópolis", "Curitiba", "Porto Alegre", "São Paulo"]

app = FastAPI(title="Mock API de crédito")


def agora() -> str:
    """Timestamp UTC em formato fixo (ordena corretamente como texto)."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


@contextmanager
def conn():
    c = sqlite3.connect(DB_PATH)
    c.row_factory = sqlite3.Row
    try:
        yield c
        c.commit()
    finally:
        c.close()


DDL = """
CREATE TABLE IF NOT EXISTS clientes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    nome TEXT, renda_mensal REAL, cidade TEXT, score INTEGER,
    created_at TEXT, updated_at TEXT, is_deleted INTEGER DEFAULT 0);
CREATE TABLE IF NOT EXISTS contratos (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    cliente_id INTEGER, valor_principal REAL, taxa_mensal REAL, n_parcelas INTEGER,
    status TEXT, created_at TEXT, updated_at TEXT, is_deleted INTEGER DEFAULT 0);
CREATE TABLE IF NOT EXISTS parcelas (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    contrato_id INTEGER, numero INTEGER, valor REAL, vencimento TEXT,
    data_pagamento TEXT, status TEXT,
    created_at TEXT, updated_at TEXT, is_deleted INTEGER DEFAULT 0);
CREATE TABLE IF NOT EXISTS deletions (
    entidade TEXT, id INTEGER, deleted_at TEXT);
"""

os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
with conn() as _c:
    _c.executescript(DDL)


def _valida(entidade: str):
    if entidade not in ENTIDADES:
        raise HTTPException(404, f"entidade inexistente: {entidade}")


def _tem_coluna(c, tabela: str, coluna: str) -> bool:
    return any(r["name"] == coluna for r in c.execute(f"PRAGMA table_info({tabela})"))


# ---------------------------------------------------------------- geração de dados
def _criar_cliente(c) -> int:
    ts = agora()
    cur = c.execute(
        "INSERT INTO clientes (nome, renda_mensal, cidade, score, created_at, updated_at) VALUES (?,?,?,?,?,?)",
        (f"Cliente {random.randint(1000, 9999)}", round(random.uniform(1800, 15000), 2),
         random.choice(CIDADES), random.randint(300, 900), ts, ts))
    return cur.lastrowid


def _criar_contrato(c, cliente_id: int, dias_atras: int = 0) -> int:
    """Cria contrato + parcelas. dias_atras > 0 cria histórico (parcelas já vencidas)."""
    n = random.choice([6, 12, 24])
    principal = round(random.uniform(2000, 50000), 2)
    taxa = round(random.uniform(0.015, 0.045), 4)
    ts = agora()
    cols = "cliente_id, valor_principal, taxa_mensal, n_parcelas, status, created_at, updated_at"
    vals = [cliente_id, principal, taxa, n, "ativo", ts, ts]
    if _tem_coluna(c, "contratos", "canal_origem"):  # presente só após /admin/schema-drift
        cols += ", canal_origem"
        vals.append(random.choice(["app", "agencia", "parceiro"]))
    cid = c.execute(f"INSERT INTO contratos ({cols}) VALUES ({','.join('?' * len(vals))})", vals).lastrowid

    valor = round(principal * (1 + taxa * n) / n, 2)
    inicio = date.today() - timedelta(days=dias_atras)
    for k in range(n):
        venc = inicio + timedelta(days=30 * (k + 1))
        if venc < date.today():
            status = random.choices(["paga", "atrasada"], weights=[7, 3])[0]
        else:
            status = "pendente"
        pago = venc.isoformat() if status == "paga" else None
        c.execute(
            "INSERT INTO parcelas (contrato_id, numero, valor, vencimento, data_pagamento, status, created_at, updated_at)"
            " VALUES (?,?,?,?,?,?,?,?)",
            (cid, k + 1, valor, venc.isoformat(), pago, status, ts, ts))
    return cid


# ---------------------------------------------------------------- eventos de mudança
def _ev_novo_cliente(c):
    _criar_cliente(c)


def _ev_novo_contrato(c):
    r = c.execute("SELECT id FROM clientes WHERE is_deleted=0 ORDER BY RANDOM() LIMIT 1").fetchone()
    if r:
        _criar_contrato(c, r["id"])


def _ev_pagar_parcela(c):
    r = c.execute("SELECT id, contrato_id FROM parcelas WHERE status IN ('pendente','atrasada') "
                  "AND is_deleted=0 ORDER BY RANDOM() LIMIT 1").fetchone()
    if not r:
        return
    ts = agora()
    c.execute("UPDATE parcelas SET status='paga', data_pagamento=?, updated_at=? WHERE id=?",
              (date.today().isoformat(), ts, r["id"]))
    abertas = c.execute("SELECT COUNT(*) FROM parcelas WHERE contrato_id=? AND status!='paga' AND is_deleted=0",
                        (r["contrato_id"],)).fetchone()[0]
    if abertas == 0:  # mudança em cascata: parcela muda e o contrato também
        c.execute("UPDATE contratos SET status='quitado', updated_at=? WHERE id=?", (ts, r["contrato_id"]))


def _ev_atrasar_parcela(c):
    r = c.execute("SELECT id, contrato_id FROM parcelas WHERE status='pendente' AND is_deleted=0 "
                  "ORDER BY RANDOM() LIMIT 1").fetchone()
    if not r:
        return
    ts = agora()
    venc = (date.today() - timedelta(days=5)).isoformat()  # simula a passagem do tempo
    c.execute("UPDATE parcelas SET status='atrasada', vencimento=?, updated_at=? WHERE id=?", (venc, ts, r["id"]))
    c.execute("UPDATE contratos SET status='inadimplente', updated_at=? WHERE id=? AND status='ativo'",
              (ts, r["contrato_id"]))


def _ev_atualizar_cliente(c):
    r = c.execute("SELECT id FROM clientes WHERE is_deleted=0 ORDER BY RANDOM() LIMIT 1").fetchone()
    if r:
        c.execute("UPDATE clientes SET renda_mensal=?, cidade=?, score=?, updated_at=? WHERE id=?",
                  (round(random.uniform(1800, 15000), 2), random.choice(CIDADES),
                   random.randint(300, 900), agora(), r["id"]))


def _ev_cancelar_contrato(c):
    r = c.execute("SELECT id FROM contratos WHERE status='ativo' AND is_deleted=0 ORDER BY RANDOM() LIMIT 1").fetchone()
    if r:
        c.execute("UPDATE contratos SET status='cancelado', updated_at=? WHERE id=?", (agora(), r["id"]))


def _ev_hard_delete_parcela(c):
    r = c.execute("SELECT id FROM parcelas ORDER BY RANDOM() LIMIT 1").fetchone()
    if r:
        _apagar(c, "parcelas", r["id"], hard=True)


EVENTOS = {  # nome: (função, peso)
    "novo_cliente": (_ev_novo_cliente, 2),
    "novo_contrato": (_ev_novo_contrato, 3),
    "pagar_parcela": (_ev_pagar_parcela, 6),
    "atrasar_parcela": (_ev_atrasar_parcela, 3),
    "atualizar_cliente": (_ev_atualizar_cliente, 3),
    "cancelar_contrato": (_ev_cancelar_contrato, 1),
    "hard_delete_parcela": (_ev_hard_delete_parcela, 1),
}


def _apagar(c, entidade: str, item_id: int, hard: bool) -> bool:
    existe = c.execute(f"SELECT 1 FROM {entidade} WHERE id=?", (item_id,)).fetchone()
    if not existe:
        return False
    ts = agora()
    if hard:  # a linha desaparece; só o tombstone avisa quem consome a API
        c.execute(f"DELETE FROM {entidade} WHERE id=?", (item_id,))
        c.execute("INSERT INTO deletions VALUES (?,?,?)", (entidade, item_id, ts))
    else:     # soft delete: a linha continua na API com is_deleted=1
        c.execute(f"UPDATE {entidade} SET is_deleted=1, updated_at=? WHERE id=?", (ts, item_id))
    return True


# ---------------------------------------------------------------- endpoints de dados
@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/v1/{entidade}/deletions")
def listar_exclusoes(entidade: str, deleted_since: str | None = None):
    _valida(entidade)
    sql, params = "SELECT id, deleted_at FROM deletions WHERE entidade=?", [entidade]
    if deleted_since:
        sql += " AND deleted_at >= ?"
        params.append(deleted_since)
    with conn() as c:
        return {"data": [dict(r) for r in c.execute(sql + " ORDER BY deleted_at, id", params)]}


@app.get("/v1/{entidade}")
def listar(entidade: str, updated_since: str | None = None,
           page: int = Query(1, ge=1), page_size: int = Query(100, ge=1, le=1000)):
    """Leitura incremental: updated_since usa >= (o ingestor deve tolerar sobreposição)."""
    _valida(entidade)
    where, params = "", []
    if updated_since:
        where, params = "WHERE updated_at >= ?", [updated_since]
    with conn() as c:
        total = c.execute(f"SELECT COUNT(*) FROM {entidade} {where}", params).fetchone()[0]
        rows = c.execute(f"SELECT * FROM {entidade} {where} ORDER BY updated_at, id LIMIT ? OFFSET ?",
                         params + [page_size, (page - 1) * page_size]).fetchall()
    return {"data": [dict(r) for r in rows], "page": page, "page_size": page_size,
            "total": total, "has_more": page * page_size < total}


@app.delete("/v1/{entidade}/{item_id}")
def apagar(entidade: str, item_id: int, hard: bool = False):
    _valida(entidade)
    with conn() as c:
        if not _apagar(c, entidade, item_id, hard):
            raise HTTPException(404, "registro não encontrado")
    return {"deleted": item_id, "hard": hard}


# ---------------------------------------------------------------- endpoints de simulação
@app.post("/admin/seed")
def seed(clientes: int = Query(20, ge=1, le=500)):
    """Carga inicial: clientes com 1-2 contratos que já têm histórico de pagamentos."""
    with conn() as c:
        for _ in range(clientes):
            cli = _criar_cliente(c)
            for _ in range(random.choice([1, 2])):
                _criar_contrato(c, cli, dias_atras=random.choice([0, 60, 120, 180]))
    return {"clientes_criados": clientes}


@app.post("/admin/simulate")
def simulate(eventos: int = Query(10, ge=1, le=500), seed_aleatoria: int | None = None):
    """Aplica N mudanças aleatórias, simulando um dia de operação do sistema de origem."""
    if seed_aleatoria is not None:
        random.seed(seed_aleatoria)  # mesma seed = mesmos eventos (útil para reproduzir bugs)
    nomes = list(EVENTOS)
    escolhidos = random.choices(nomes, weights=[EVENTOS[n][1] for n in nomes], k=eventos)
    with conn() as c:
        for n in escolhidos:
            EVENTOS[n][0](c)
    return {"eventos_aplicados": dict(Counter(escolhidos))}


@app.post("/admin/schema-drift")
def schema_drift():
    """Adiciona a coluna canal_origem em contratos: a fonte mudou o contrato da API."""
    with conn() as c:
        if not _tem_coluna(c, "contratos", "canal_origem"):
            c.execute("ALTER TABLE contratos ADD COLUMN canal_origem TEXT")
    return {"coluna_adicionada": "contratos.canal_origem"}
