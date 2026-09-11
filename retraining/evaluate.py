"""Calcul des métriques de qualité de prédiction et verdict par titre.

Verdict OK si (et seulement si) les TROIS conditions sont réunies :
    - MAPE < 10 %
    - MAE relative < 10 %   (MAE / cours moyen)
    - Accuracy directionnelle > 70 %
Sinon le modèle est jugé "defaillant" et sera réestimé.
"""
import numpy as np

THRESHOLDS = {"mape_max": 10.0, "rel_mae_max": 10.0, "dir_acc_min": 70.0}
MIN_POINTS = 5  # en dessous, jugement non fiable -> "insuffisant", pas de réestimation


def compute_metrics(preds, actuals):
    """preds, actuals : listes de dicts (voir db.fetch_*).

    Retourne un dict de métriques, ou {"insufficient": True} si trop peu de points.
    """
    if not preds or not actuals:
        return {"n": 0, "insufficient": True}

    actual_by_date = {str(r["trade_date"]): float(r["price"]) for r in actuals}
    sorted_dates = sorted(actual_by_date)

    # Dernier run_date par prediction_date (la prévision la plus récente pour cette date)
    latest = {}
    for r in preds:
        d = str(r["prediction_date"])
        rd = str(r.get("run_date") or "")
        if d not in latest or rd > latest[d][0]:
            latest[d] = (rd, float(r["predicted_price"]))

    pairs = []  # (pred, actual, prev_actual)
    for d, (_, pred) in latest.items():
        if d in actual_by_date:
            prev = [x for x in sorted_dates if x < d]
            prev_actual = actual_by_date[prev[-1]] if prev else None
            pairs.append((pred, actual_by_date[d], prev_actual))

    pairs = [p for p in pairs if p[1] and p[1] != 0.0]
    n = len(pairs)
    if n < MIN_POINTS:
        return {"n": n, "insufficient": True}

    pred = np.array([p[0] for p in pairs], dtype=float)
    act = np.array([p[1] for p in pairs], dtype=float)
    err = pred - act

    rmse = float(np.sqrt(np.mean(err ** 2)))
    mae = float(np.mean(np.abs(err)))
    mape = float(np.mean(np.abs(err) / np.abs(act)) * 100.0)
    rel_mae = float(mae / np.mean(act) * 100.0)

    directional = [p for p in pairs if p[2] is not None]
    if directional:
        pred_dir = np.sign([p[0] - p[2] for p in directional])
        act_dir = np.sign([p[1] - p[2] for p in directional])
        dir_acc = float(np.mean(pred_dir == act_dir) * 100.0)
    else:
        dir_acc = None

    return {
        "n": n,
        "insufficient": False,
        "rmse": rmse,
        "mae": mae,
        "mape": mape,
        "rel_mae": rel_mae,
        "dir_acc": dir_acc,
    }


def verdict(m):
    """Retourne (verdict, raisons). verdict ∈ {ok, defaillant, insuffisant}."""
    if not m or m.get("insufficient"):
        return "insuffisant", ["données insuffisantes pour juger"]

    reasons = []
    if m["mape"] >= THRESHOLDS["mape_max"]:
        reasons.append(f"MAPE {m['mape']:.1f}% ≥ {THRESHOLDS['mape_max']:.0f}%")
    if m["rel_mae"] >= THRESHOLDS["rel_mae_max"]:
        reasons.append(f"MAE rel. {m['rel_mae']:.1f}% ≥ {THRESHOLDS['rel_mae_max']:.0f}%")
    if m["dir_acc"] is not None and not (m["dir_acc"] > THRESHOLDS["dir_acc_min"]):
        reasons.append(f"Acc. dir. {m['dir_acc']:.1f}% ≤ {THRESHOLDS['dir_acc_min']:.0f}%")

    return ("ok" if not reasons else "defaillant"), reasons
