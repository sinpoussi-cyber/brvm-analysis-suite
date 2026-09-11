"""Construction et envoi du mail de récapitulatif (Gmail/SMTP)."""
import os
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart


def _fmt(v, suffix="", nd=1):
    if v is None:
        return "—"
    try:
        return f"{float(v):.{nd}f}{suffix}"
    except (TypeError, ValueError):
        return str(v)


def _badge(verdict):
    colors = {"ok": "#1a7f37", "defaillant": "#cf222e", "insuffisant": "#9a6700"}
    labels = {"ok": "OK", "defaillant": "DÉFAILLANT", "insuffisant": "INSUFFISANT"}
    c = colors.get(verdict, "#57606a")
    return (f'<span style="background:{c};color:#fff;padding:2px 8px;'
            f'border-radius:10px;font-size:12px;font-weight:600;">'
            f'{labels.get(verdict, verdict)}</span>')


def build_html(results, period_label, replaced):
    """results : liste de dicts (voir run.build_result). replaced : liste de tickers."""
    n = len(results)
    n_ok = sum(1 for r in results if r["verdict"] == "ok")
    n_def = sum(1 for r in results if r["verdict"] == "defaillant")
    n_ins = sum(1 for r in results if r["verdict"] == "insuffisant")

    rows_html = []
    for r in sorted(results, key=lambda x: (x["verdict"] != "defaillant", x["ticker"])):
        m = r["metrics"] or {}
        prev = r.get("previous") or {}
        prev_mape = prev.get("mape")
        prev_dir = prev.get("dir_accuracy")
        reason = "<br>".join(r["reasons"]) if r["reasons"] else "—"
        rep = "✅ remplacé" if r["ticker"] in replaced else (
            r.get("retrain", {}).get("reason", "—") if r["verdict"] == "defaillant" else "—")
        rows_html.append(f"""
        <tr>
          <td style="font-weight:600">{r['ticker']}</td>
          <td>{_badge(r['verdict'])}</td>
          <td>{_fmt(m.get('mape'), '%')}</td>
          <td>{_fmt(m.get('rel_mae'), '%')}</td>
          <td>{_fmt(m.get('dir_acc'), '%')}</td>
          <td>{_fmt(m.get('rmse'), '', 3)}</td>
          <td>{_fmt(m.get('mae'), '', 3)}</td>
          <td>{m.get('n', '—')}</td>
          <td style="color:#57606a">{_fmt(prev_mape, '%')} / {_fmt(prev_dir, '%')}</td>
          <td style="font-size:12px">{reason}</td>
          <td style="font-size:12px">{rep}</td>
        </tr>""")

    replaced_html = ", ".join(f"<code>modeles/{t}</code>" for t in replaced) or "aucun"

    return f"""<html><body style="font-family:Segoe UI,Arial,sans-serif;color:#1f2328">
    <h2 style="margin-bottom:4px">Réestimation mensuelle des modèles BRVM</h2>
    <p style="color:#57606a;margin-top:0">Période évaluée : <b>{period_label}</b></p>

    <table cellpadding="6" style="border-collapse:collapse;margin:12px 0">
      <tr>
        <td style="background:#f6f8fa;border-radius:6px">Titres évalués<br><b style="font-size:20px">{n}</b></td>
        <td style="background:#dafbe1;border-radius:6px">OK<br><b style="font-size:20px">{n_ok}</b></td>
        <td style="background:#ffebe9;border-radius:6px">Défaillants<br><b style="font-size:20px">{n_def}</b></td>
        <td style="background:#fff8c5;border-radius:6px">Insuffisants<br><b style="font-size:20px">{n_ins}</b></td>
        <td style="background:#ddf4ff;border-radius:6px">Dossiers remplacés<br><b style="font-size:20px">{len(replaced)}</b></td>
      </tr>
    </table>

    <p><b>Dossiers remplacés :</b> {replaced_html}</p>

    <h3>Analyse détaillée de la qualité actuelle</h3>
    <p style="color:#57606a;font-size:13px">Seuils : MAPE &lt; 10 % ET MAE rel. &lt; 10 % ET Acc. dir. &gt; 70 %.
    Colonne « Précédent » = MAPE / Acc. dir. de la dernière évaluation.</p>
    <table cellpadding="6" style="border-collapse:collapse;width:100%;font-size:13px">
      <tr style="background:#f6f8fa;text-align:left">
        <th>Titre</th><th>Verdict</th><th>MAPE</th><th>MAE rel.</th><th>Acc. dir.</th>
        <th>RMSE</th><th>MAE</th><th>n</th><th>Précédent</th><th>Raisons</th><th>Action</th>
      </tr>
      {''.join(rows_html)}
    </table>

    <p style="color:#8c959f;font-size:12px;margin-top:20px">
      Généré automatiquement par le workflow monthly-retrain de brvm-analysis-suite.</p>
    </body></html>"""


def send_email(subject, html):
    user = os.environ["GMAIL_USER"]
    pw = os.environ["GMAIL_APP_PASSWORD"]
    to = os.environ.get("MAIL_TO", user)
    recipients = [x.strip() for x in to.split(",") if x.strip()]

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = user
    msg["To"] = ", ".join(recipients)
    msg.attach(MIMEText(html, "html"))

    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
        server.login(user, pw)
        server.sendmail(user, recipients, msg.as_string())
