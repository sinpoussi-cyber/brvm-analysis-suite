"""Orchestrateur du pipeline mensuel.

Étapes :
 1. Repère les titres depuis modeles/<TICKER>/model_GRU.keras
 2. Mappe ticker -> company_id via la table companies (auto-détection)
 3. Pour chaque titre : évalue la qualité sur le mois civil précédent
 4. Si "defaillant" : warm-start fine-tuning sur les 300 dernières cotations
    (ou tout l'historique disponible si le titre est coté depuis moins longtemps)
 5. Historise les évaluations dans model_evaluations
 6. Envoie le mail de récapitulatif

Le commit des .keras modifiés est fait par le workflow GitHub Actions.
"""
import os
import sys
from datetime import date, timedelta

import db
import evaluate as ev
import retrain as rt
import notify


MODELS_DIR = os.environ.get("MODELS_DIR", "modeles")
MODEL_FILE = "model_GRU.keras"
SCALER_FILE = "scaler.pkl"


def previous_month_range(today=None):
    today = today or date.today()
    first_this_month = today.replace(day=1)
    end = first_this_month - timedelta(days=1)      # dernier jour du mois précédent
    start = end.replace(day=1)                       # premier jour du mois précédent
    return start, end


def discover_tickers():
    tickers = []
    if not os.path.isdir(MODELS_DIR):
        return tickers
    for name in sorted(os.listdir(MODELS_DIR)):
        d = os.path.join(MODELS_DIR, name)
        if os.path.isdir(d) and os.path.exists(os.path.join(d, MODEL_FILE)):
            tickers.append(name)
    return tickers


def build_result(ticker, metrics, verdict, reasons, previous, retrain_info):
    return {
        "ticker": ticker,
        "metrics": metrics,
        "verdict": verdict,
        "reasons": reasons,
        "previous": previous,
        "retrain": retrain_info or {},
    }


def main():
    client = db.get_client()
    tickers = discover_tickers()
    if not tickers:
        print(f"Aucun modèle trouvé dans {MODELS_DIR}/", file=sys.stderr)
        return 1

    ticker_to_id, _ = db.load_company_map(client, tickers)
    start, end = previous_month_range()
    period_label = f"{start.isoformat()} → {end.isoformat()}"
    # marge amont pour disposer du cours de la veille (accuracy directionnelle)
    actuals_start = start - timedelta(days=15)

    results, eval_rows, replaced = [], [], []
    today = date.today()

    for ticker in tickers:
        cid = ticker_to_id.get(ticker)
        if cid is None:
            results.append(build_result(
                ticker, None, "insuffisant",
                ["company_id introuvable dans companies"], None, None))
            continue

        preds = db.fetch_predictions(client, cid, start, end)
        actuals = db.fetch_actuals(client, cid, actuals_start, end)
        metrics = ev.compute_metrics(preds, actuals)
        verdict, reasons = ev.verdict(metrics)
        previous = db.fetch_last_eval(client, ticker)

        retrain_info = None
        if verdict == "defaillant":
            model_path = os.path.join(MODELS_DIR, ticker, MODEL_FILE)
            scaler_path = os.path.join(MODELS_DIR, ticker, SCALER_FILE)
            recent = db.fetch_recent(client, cid, n=300)
            retrain_info = rt.warm_start_finetune(model_path, scaler_path, recent)
            if retrain_info.get("status") == "retrained":
                replaced.append(ticker)
            print(f"[{ticker}] defaillant -> {retrain_info}")
        else:
            print(f"[{ticker}] {verdict} ({', '.join(reasons) or 'seuils respectés'})")

        results.append(build_result(ticker, metrics, verdict, reasons, previous, retrain_info))

        m = metrics or {}
        eval_rows.append({
            "ticker": ticker,
            "eval_date": today.isoformat(),
            "rmse": m.get("rmse"),
            "mae": m.get("mae"),
            "mape": m.get("mape"),
            "dir_accuracy": m.get("dir_acc"),
            "verdict": verdict,
            "replaced": ticker in replaced,
        })

    db.insert_evaluations(client, eval_rows)

    html = notify.build_html(results, period_label, replaced)
    subject = f"[BRVM] Réestimation mensuelle — {len(replaced)} modèle(s) remplacé(s) — {period_label}"
    try:
        notify.send_email(subject, html)
        print("Mail de récapitulatif envoyé.")
    except Exception as exc:  # noqa: BLE001
        print(f"Échec envoi mail : {exc}", file=sys.stderr)

    print(f"Terminé : {len(results)} titres, {len(replaced)} remplacés.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
