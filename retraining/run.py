"""Orchestrateur du pipeline mensuel.

Étapes :
 1. Repère les titres depuis modeles/<TICKER>/model_GRU.keras
 2. Mappe ticker -> company_id via la table companies (auto-détection)
 3. Pour chaque titre : évalue la qualité sur le mois civil précédent
 4. Si "defaillant" : warm-start fine-tuning sur les 300 dernières cotations
    (ou tout l'historique disponible si le titre est coté depuis moins longtemps)
 5. Historise les évaluations dans model_evaluations
 6. Entraînement initial des titres sans modèle (nouvelles cotations, ex. BBGC)
    dès qu'ils ont assez d'historique — voir cold_start.py
 7. Envoie le mail de récapitulatif

Le commit des .keras modifiés est fait par le workflow GitHub Actions.
"""
import os
import sys
from datetime import date, timedelta

import db
import evaluate as ev
import retrain as rt
import notify
import cold_start as cs


MODELS_DIR = os.environ.get("MODELS_DIR", "modeles")


def previous_month_range(today=None):
    today = today or date.today()
    first_this_month = today.replace(day=1)
    end = first_this_month - timedelta(days=1)      # dernier jour du mois précédent
    start = end.replace(day=1)                       # premier jour du mois précédent
    return start, end


def discover_models():
    """Repère les modèles dans modeles/<TICKER>/.

    Le dépôt mélange plusieurs conventions de nommage (model_GRU.keras,
    model_GRU_advanced.keras, model_BiGRU_advanced.keras, model_LSTM.keras ;
    scaler.pkl ou scaler_advanced.pkl). On détecte donc le fichier .keras et le
    fichier .pkl présents dans chaque dossier au lieu d'un nom figé, et le
    réentraînement ré-enregistre sous le même nom.

    Retourne {ticker: (model_filename, scaler_filename)}.
    """
    found = {}
    if not os.path.isdir(MODELS_DIR):
        return found
    for name in sorted(os.listdir(MODELS_DIR)):
        d = os.path.join(MODELS_DIR, name)
        if not os.path.isdir(d):
            continue
        keras_files = sorted(f for f in os.listdir(d) if f.endswith(".keras"))
        pkl_files = sorted(f for f in os.listdir(d) if f.endswith(".pkl"))
        if keras_files and pkl_files:
            found[name] = (keras_files[0], pkl_files[0])
        else:
            manque = "aucun .keras" if not keras_files else "aucun .pkl"
            print(f"[{name}] ignoré : {manque} dans le dossier")
    return found


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
    models = discover_models()
    if not models:
        print(f"Aucun modèle trouvé dans {MODELS_DIR}/ — seuls les nouveaux titres seront traités",
              file=sys.stderr)
    tickers = list(models.keys())

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
            model_file, scaler_file = models[ticker]
            model_path = os.path.join(MODELS_DIR, ticker, model_file)
            scaler_path = os.path.join(MODELS_DIR, ticker, scaler_file)
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

    # ── Nouveaux titres : sociétés cotées sans dossier modeles/<TICKER>/ ──
    new_titles = []
    all_companies = db.list_companies(client)
    for ticker, cid in sorted(all_companies.items()):
        if ticker in models or os.path.isdir(os.path.join(MODELS_DIR, ticker)):
            continue
        rows = db.fetch_recent(client, cid, n=5000)   # tout l'historique
        info = cs.train_new_model(ticker, rows, MODELS_DIR)
        info["ticker"] = ticker
        new_titles.append(info)
        print(f"[{ticker}] nouveau titre -> {info}")
    created = [t["ticker"] for t in new_titles if t["status"] == "trained"]

    html = notify.build_html(results, period_label, replaced, new_titles)
    subject = (f"[BRVM] Réestimation mensuelle — {len(replaced)} modèle(s) remplacé(s)"
               + (f", {len(created)} créé(s)" if created else "")
               + f" — {period_label}")
    try:
        notify.send_email(subject, html)
        print("Mail de récapitulatif envoyé.")
    except Exception as exc:  # noqa: BLE001
        print(f"Échec envoi mail : {exc}", file=sys.stderr)

    print(f"Terminé : {len(results)} titres, {len(replaced)} remplacés, "
          f"{len(created)} créés, {len(new_titles) - len(created)} nouveaux titres en attente/erreur.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
