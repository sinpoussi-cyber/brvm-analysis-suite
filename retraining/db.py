"""Accès Supabase pour le pipeline de réestimation mensuelle des modèles BRVM.

Toutes les fonctions attendent un client obtenu via get_client() et travaillent
sur les tables : companies, predictions, historical_data, model_evaluations.
"""
import os
from supabase import create_client, Client


def get_client() -> Client:
    url = os.environ["SUPABASE_URL"]
    key = os.environ["SUPABASE_KEY"]
    return create_client(url, key)


def load_company_map(client: Client, model_tickers):
    """Détecte automatiquement la colonne "ticker" de la table `companies`.

    On ne connaît pas le nom exact de la colonne (ticker / symbol / code / name).
    On récupère toutes les sociétés puis on choisit la colonne dont les valeurs
    recouvrent le mieux l'ensemble des noms de dossiers de `modeles/`.

    Retourne (ticker_to_id, id_to_ticker).
    """
    rows = client.table("companies").select("*").execute().data or []
    if not rows:
        return {}, {}

    wanted = {str(t).strip() for t in model_tickers}
    best_col, best_hits = None, -1
    for col in rows[0].keys():
        if col == "id":
            continue
        vals = {str(r[col]).strip() for r in rows if r.get(col) is not None}
        hits = len(wanted & vals)
        if hits > best_hits:
            best_col, best_hits = col, hits

    ticker_to_id, id_to_ticker = {}, {}
    for r in rows:
        t = str(r.get(best_col)).strip() if r.get(best_col) is not None else None
        if t in wanted:
            ticker_to_id[t] = r["id"]
            id_to_ticker[r["id"]] = t
    return ticker_to_id, id_to_ticker


def fetch_predictions(client: Client, company_id, start, end):
    """Prédictions dont prediction_date est dans [start, end]."""
    return (
        client.table("predictions")
        .select("prediction_date,predicted_price,run_date")
        .eq("company_id", company_id)
        .gte("prediction_date", start.isoformat())
        .lte("prediction_date", end.isoformat())
        .execute()
        .data
        or []
    )


def fetch_actuals(client: Client, company_id, start, end):
    """Cours réels (price non nul) dans [start, end], triés par date croissante."""
    return (
        client.table("historical_data")
        .select("trade_date,price")
        .eq("company_id", company_id)
        .gte("trade_date", start.isoformat())
        .lte("trade_date", end.isoformat())
        .not_.is_("price", "null")
        .order("trade_date")
        .execute()
        .data
        or []
    )


def fetch_recent(client: Client, company_id, n=100):
    """Les n dernières cotations (price non nul), triées par date croissante."""
    cols = "trade_date,price,volume,value,company_capitalization"
    rows = (
        client.table("historical_data")
        .select(cols)
        .eq("company_id", company_id)
        .not_.is_("price", "null")
        .order("trade_date", desc=True)
        .limit(n)
        .execute()
        .data
        or []
    )
    rows.reverse()
    return rows


def fetch_last_eval(client: Client, ticker):
    rows = (
        client.table("model_evaluations")
        .select("*")
        .eq("ticker", ticker)
        .order("eval_date", desc=True)
        .limit(1)
        .execute()
        .data
        or []
    )
    return rows[0] if rows else None


def insert_evaluations(client: Client, rows):
    if rows:
        client.table("model_evaluations").insert(rows).execute()
