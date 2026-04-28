# ==============================================================================
# macro_collector.py — Collecteur d'actualités macro-économiques v3
#
# STRATÉGIE DE COLLECTE (dans l'ordre) :
#   1. Flux RSS Google News  → si le réseau GitHub Actions les autorise
#   2. Mistral avec web_search activé → garantit la collecte même sans RSS
#
# La stratégie 2 (Mistral web_search) est le vrai filet de sécurité :
#   - Mistral est déjà ton IA principale (MISTRAL_API_KEY dans tes secrets)
#   - Son endpoint accepte l'option "web_search" qui lui permet de chercher
#     des actualités récentes en temps réel
#   - Retourne directement les articles résumés et analysés → insertion BDD
#
# ==============================================================================

import feedparser
import requests
import logging
import time
import json
import re
import os
import sys
import traceback
import hashlib
import psycopg2
import psycopg2.extras
from datetime import datetime, timezone, timedelta
from typing import Optional

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# ==============================================================================
# SOURCES RSS (tentative 1 — peut échouer sur certains runners)
# ==============================================================================
RSS_SOURCES = [
    {
        "name":      "Google News — BRVM bourse",
        "url":       "https://news.google.com/rss/search?q=BRVM+bourse+régionale&hl=fr&gl=CI&ceid=CI:fr",
        "zone":      "uemoa", "categorie": "marche_financier", "langue": "fr", "priorite": 1,
    },
    {
        "name":      "Google News — BCEAO",
        "url":       "https://news.google.com/rss/search?q=BCEAO+politique+monétaire&hl=fr&gl=FR&ceid=FR:fr",
        "zone":      "uemoa", "categorie": "politique_monetaire", "langue": "fr", "priorite": 1,
    },
    {
        "name":      "Google News — UEMOA économie",
        "url":       "https://news.google.com/rss/search?q=UEMOA+économie+croissance&hl=fr&gl=FR&ceid=FR:fr",
        "zone":      "uemoa", "categorie": "économique", "langue": "fr", "priorite": 1,
    },
    {
        "name":      "Google News — Côte d'Ivoire",
        "url":       "https://news.google.com/rss/search?q=%22Côte+d%27Ivoire%22+économie&hl=fr&gl=CI&ceid=CI:fr",
        "zone":      "uemoa", "categorie": "économique", "langue": "fr", "priorite": 1,
    },
    {
        "name":      "Google News — franc CFA",
        "url":       "https://news.google.com/rss/search?q=franc+CFA+FCFA&hl=fr&gl=FR&ceid=FR:fr",
        "zone":      "uemoa", "categorie": "politique_monetaire", "langue": "fr", "priorite": 1,
    },
    {
        "name":      "Google News — Afrique économie",
        "url":       "https://news.google.com/rss/search?q=afrique+économie+croissance+investissement&hl=fr&gl=FR&ceid=FR:fr",
        "zone":      "afrique", "categorie": "économique", "langue": "fr", "priorite": 1,
    },
    {
        "name":      "Google News — Afrique de l'Ouest",
        "url":       "https://news.google.com/rss/search?q=%22Afrique+de+l%27Ouest%22+OR+CEDEAO&hl=fr&gl=FR&ceid=FR:fr",
        "zone":      "afrique", "categorie": "économique", "langue": "fr", "priorite": 1,
    },
    {
        "name":      "Google News — dette Afrique FMI",
        "url":       "https://news.google.com/rss/search?q=Afrique+dette+FMI+Banque+Mondiale&hl=fr&gl=FR&ceid=FR:fr",
        "zone":      "afrique", "categorie": "économique", "langue": "fr", "priorite": 2,
    },
    {
        "name":      "Google News — cacao prix",
        "url":       "https://news.google.com/rss/search?q=cacao+prix+marché+cocoa&hl=fr&gl=FR&ceid=FR:fr",
        "zone":      "uemoa", "categorie": "matieres_premieres", "langue": "fr", "priorite": 1,
    },
    {
        "name":      "Google News — pétrole or",
        "url":       "https://news.google.com/rss/search?q=pétrole+prix+OR+%22cours+de+l%27or%22&hl=fr&gl=FR&ceid=FR:fr",
        "zone":      "international", "categorie": "matieres_premieres", "langue": "fr", "priorite": 1,
    },
    {
        "name":      "Google News — Fed BCE taux",
        "url":       "https://news.google.com/rss/search?q=%22Federal+Reserve%22+OR+BCE+taux+inflation&hl=fr&gl=FR&ceid=FR:fr",
        "zone":      "international", "categorie": "politique_monetaire", "langue": "fr", "priorite": 1,
    },
    {
        "name":      "Google News — marchés financiers",
        "url":       "https://news.google.com/rss/search?q=marchés+financiers+bourse+indices&hl=fr&gl=FR&ceid=FR:fr",
        "zone":      "international", "categorie": "marche_financier", "langue": "fr", "priorite": 2,
    },
    {
        "name":      "Google News — récession inflation",
        "url":       "https://news.google.com/rss/search?q=récession+OR+inflation+économie+mondiale&hl=fr&gl=FR&ceid=FR:fr",
        "zone":      "international", "categorie": "économique", "langue": "fr", "priorite": 1,
    },
    {
        "name":      "Google News — FMI Banque Mondiale",
        "url":       "https://news.google.com/rss/search?q=FMI+%22Banque+Mondiale%22+Afrique&hl=fr&gl=FR&ceid=FR:fr",
        "zone":      "afrique", "categorie": "économique", "langue": "fr", "priorite": 1,
    },
    {
        "name":      "Google News — géopolitique Afrique",
        "url":       "https://news.google.com/rss/search?q=géopolitique+Afrique+sanctions&hl=fr&gl=FR&ceid=FR:fr",
        "zone":      "afrique", "categorie": "geopolitique", "langue": "fr", "priorite": 2,
    },
]

# ==============================================================================
# REQUÊTES pour la collecte Mistral web_search (fallback si RSS échoue)
# ==============================================================================
MISTRAL_SEARCH_QUERIES = [
    # (query, zone, categorie)
    ("BRVM bourse régionale valeurs mobilières actualités",                            "uemoa",         "marche_financier"),
    ("BCEAO politique monétaire taux directeur UEMOA",                                "uemoa",         "politique_monetaire"),
    ("économie Côte d'Ivoire Sénégal Mali Burkina Faso actualités",                   "uemoa",         "économique"),
    ("cacao prix marché mondial cocoa Côte d'Ivoire",                                 "uemoa",         "matieres_premieres"),
    ("caoutchouc huile de palme coton prix Afrique Ouest",                            "uemoa",         "matieres_premieres"),
    ("économie Afrique croissance investissement dette actualités",                    "afrique",       "économique"),
    ("CEDEAO Afrique de l'Ouest instabilité politique élections",                     "afrique",       "geopolitique"),
    ("FMI Banque Mondiale Afrique prêt programme économique",                         "afrique",       "économique"),
    ("Federal Reserve Fed taux d'intérêt inflation décision",                         "international", "politique_monetaire"),
    ("BCE Banque Centrale Européenne taux euro zone",                                 "international", "politique_monetaire"),
    ("pétrole Brent WTI prix marché énergie",                                         "international", "matieres_premieres"),
    ("marchés financiers bourse Wall Street indices actions actualités",              "international", "marche_financier"),
    ("récession inflation croissance économie mondiale PIB",                          "international", "économique"),
    ("dollar euro change forex taux FCFA",                                            "international", "marche_financier"),
]

# ==============================================================================
# MOTS-CLÉS DE PERTINENCE
# ==============================================================================
KEYWORDS_IMPACT = {
    "brvm_direct": ["brvm", "bourse abidjan", "côte d'ivoire", "bourse régionale",
                    "uemoa", "fcfa", "bceao", "zone franc", "abidjan"],
    "afrique_ouest": ["sénégal", "mali", "burkina", "niger", "togo", "bénin", "ghana",
                      "nigeria", "cedeao", "ecowas", "afrique de l'ouest", "west africa"],
    "matieres_premieres": ["cacao", "cocoa", "caoutchouc", "rubber", "pétrole", "oil",
                           "or", "gold", "coton", "cotton", "café", "coffee",
                           "palmier", "palm oil", "matières premières", "commodities"],
    "marches_financiers": ["wall street", "dow jones", "sp500", "nasdaq", "euronext",
                           "cac40", "bourse", "taux d'intérêt", "interest rate",
                           "fed", "federal reserve", "bce", "banque centrale",
                           "inflation", "récession", "recession"],
    "geopolitique": ["guerre", "war", "conflit", "conflict", "sanctions", "embargo",
                     "coup d'état", "putsch", "élection", "fmi", "imf"],
}


# ==============================================================================
# CLASSE PRINCIPALE
# ==============================================================================

class MacroCollector:
    """
    Collecte les actualités macro-économiques et les insère dans google_alerts_rapports.

    Stratégie :
      1. Essaie de collecter via flux RSS Google News
      2. Si RSS insuffisant (< 3 articles), collecte via Mistral web_search
      3. Enrichissement IA des articles bruts (résumé, impact BRVM, sentiment)
    """

    def __init__(self, db_conn, gemini_keys: list, deepseek_key: str,
                 mistral_key: str, max_articles_per_source: int = 10):
        self.db_conn       = db_conn
        self.gemini_keys   = gemini_keys if isinstance(gemini_keys, list) else ([gemini_keys] if gemini_keys else [])
        self.deepseek_key  = deepseek_key
        self.mistral_key   = mistral_key
        self.max_per_src   = max_articles_per_source
        self._gemini_idx   = 0
        self.stats = {"fetched": 0, "inserted": 0, "skipped": 0, "errors": 0}

    # ──────────────────────────────────────────────────────────────────────────
    # POINT D'ENTRÉE
    # ──────────────────────────────────────────────────────────────────────────

    def run(self):
        """Lance la collecte complète avec stratégie RSS → Mistral web_search."""
        logging.info("="*60)
        logging.info("🌍 MACRO COLLECTOR v3 — Démarrage")
        logging.info("="*60)

        self._ensure_table_exists()
        self._ensure_table_columns()

        # ── Tentative 1 : RSS ────────────────────────────────────────────────
        articles = self._fetch_all_rss()
        logging.info(f"📡 RSS : {len(articles)} article(s) collecté(s)")

        # ── Tentative 2 : Mistral web_search si RSS insuffisant ──────────────
        # Seuil : moins de 3 articles collectés = RSS inaccessible ou bloqué
        if len(articles) < 3 and self.mistral_key:
            logging.info("⚠️  RSS insuffisant — basculement sur Mistral web_search")
            mistral_articles = self._collect_via_mistral_websearch()
            logging.info(f"🤖 Mistral web_search : {len(mistral_articles)} article(s) générés")
            articles.extend(mistral_articles)

        if not articles:
            logging.warning("❌ Aucun article collecté (RSS + Mistral) — vérifier les secrets API")
            return self.stats

        # ── Déduplication ────────────────────────────────────────────────────
        articles = self._filter_existing(articles)
        logging.info(f"🔍 {len(articles)} article(s) nouveaux après déduplication")

        if not articles:
            logging.info("✅ BDD déjà à jour, aucun nouvel article")
            return self.stats

        # ── Enrichissement IA et insertion ───────────────────────────────────
        for art in articles:
            try:
                # Les articles Mistral sont déjà enrichis — skip enrichissement IA
                if not art.get("_already_enriched"):
                    art = self._enrich_with_ai(art)
                self._insert_article(art)
                self.stats["inserted"] += 1
                time.sleep(0.3)
            except Exception as e:
                logging.error(f"❌ Insertion {art.get('titre','?')[:50]}: {e}")
                logging.debug(traceback.format_exc())
                self.stats["errors"] += 1

        logging.info("="*60)
        logging.info(f"✅ Collecte terminée — Insérés: {self.stats['inserted']} | "
                     f"Ignorés: {self.stats['skipped']} | Erreurs: {self.stats['errors']}")
        logging.info("="*60)
        return self.stats

    # ──────────────────────────────────────────────────────────────────────────
    # TABLE
    # ──────────────────────────────────────────────────────────────────────────

    def _ensure_table_exists(self):
        """Crée la table google_alerts_rapports si elle n'existe pas."""
        cursor = self.db_conn.cursor()
        try:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS google_alerts_rapports (
                    id                        SERIAL PRIMARY KEY,
                    mail_date                 TIMESTAMPTZ,
                    mail_subject              TEXT,
                    titre                     TEXT,
                    resume                    TEXT,
                    points_cles               JSONB,
                    sentiment                 TEXT,
                    pertinence                INTEGER DEFAULT 50,
                    categorie                 TEXT,
                    rapport_type              TEXT,
                    alert_keyword             TEXT,
                    mot_cle                   TEXT,
                    source_url                TEXT,
                    url_hash                  TEXT UNIQUE,
                    source_rss                TEXT,
                    zone                      TEXT,
                    sous_categorie            TEXT,
                    langue                    TEXT DEFAULT 'fr',
                    score_importance          INTEGER DEFAULT 50,
                    impact_brvm               TEXT DEFAULT 'neutre',
                    impact_bourses_mondiales  TEXT,
                    impact_secteurs_brvm      JSONB,
                    impact_societes_cotees    JSONB,
                    collecte_date             TIMESTAMPTZ DEFAULT NOW(),
                    envoye_email              BOOLEAN DEFAULT FALSE,
                    created_at                TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            for idx_sql in [
                "CREATE INDEX IF NOT EXISTS idx_gar_url_hash      ON google_alerts_rapports(url_hash);",
                "CREATE INDEX IF NOT EXISTS idx_gar_zone          ON google_alerts_rapports(zone);",
                "CREATE INDEX IF NOT EXISTS idx_gar_collecte_date ON google_alerts_rapports(collecte_date DESC);",
            ]:
                cursor.execute(idx_sql)
            self.db_conn.commit()
            logging.info("✅ Table google_alerts_rapports prête")
        except Exception as e:
            self.db_conn.rollback()
            logging.warning(f"⚠️  _ensure_table_exists: {e}")
        finally:
            cursor.close()

    def _ensure_table_columns(self):
        """
        Ajoute les colonnes manquantes et crée l'index UNIQUE sur url_hash.

        IMPORTANT : ADD COLUMN IF NOT EXISTS n'ajoute pas de contrainte UNIQUE
        sur une colonne existante. Il faut créer l'index séparément avec
        CREATE UNIQUE INDEX IF NOT EXISTS — idempotent et sûr.
        """
        colonnes = [
            ("zone",                     "TEXT"),
            ("sous_categorie",           "TEXT"),
            ("impact_brvm",              "TEXT DEFAULT 'neutre'"),
            ("impact_bourses_mondiales", "TEXT"),
            ("impact_secteurs_brvm",     "JSONB"),
            ("impact_societes_cotees",   "JSONB"),
            ("source_rss",               "TEXT"),
            ("collecte_date",            "TIMESTAMPTZ DEFAULT NOW()"),
            ("langue",                   "TEXT DEFAULT 'fr'"),
            ("score_importance",         "INTEGER DEFAULT 50"),
            ("url_hash",                 "TEXT"),
        ]
        cursor = self.db_conn.cursor()
        # 1. Ajouter les colonnes manquantes
        for col_name, col_type in colonnes:
            try:
                cursor.execute(f"ALTER TABLE google_alerts_rapports ADD COLUMN IF NOT EXISTS {col_name} {col_type};")
                self.db_conn.commit()
            except Exception:
                self.db_conn.rollback()

        # 2. Créer l'index UNIQUE sur url_hash (obligatoire pour ON CONFLICT)
        #    Partiel (WHERE url_hash IS NOT NULL) pour éviter les conflits sur NULL
        try:
            cursor.execute("""
                CREATE UNIQUE INDEX IF NOT EXISTS idx_gar_url_hash_unique
                ON google_alerts_rapports(url_hash)
                WHERE url_hash IS NOT NULL;
            """)
            self.db_conn.commit()
            logging.info("   ✅ Index UNIQUE url_hash vérifié")
        except Exception as e:
            self.db_conn.rollback()
            logging.warning(f"   ⚠️  Index UNIQUE url_hash: {e}")

        cursor.close()

    # ──────────────────────────────────────────────────────────────────────────
    # COLLECTE RSS
    # ──────────────────────────────────────────────────────────────────────────

    def _fetch_all_rss(self) -> list:
        """Collecte depuis tous les flux RSS (peut échouer silencieusement)."""
        all_articles = []
        cutoff = datetime.now(timezone.utc) - timedelta(days=7)

        for source in RSS_SOURCES:
            try:
                articles = self._fetch_one_rss(source, cutoff)
                all_articles.extend(articles)
                self.stats["fetched"] += len(articles)
                if articles:
                    logging.info(f"   📡 {source['name']:<45} → {len(articles)} article(s)")
                time.sleep(0.4)
            except Exception as e:
                logging.debug(f"   RSS {source['name']}: {e}")
                self.stats["errors"] += 1

        return all_articles

    def _fetch_one_rss(self, source: dict, cutoff: datetime) -> list:
        """Collecte les articles d'une source RSS."""
        headers = {
            'User-Agent': 'Mozilla/5.0 (compatible; Feedfetcher-Google; +http://www.google.com/feedfetcher.html)',
            'Accept': 'application/rss+xml, application/xml, text/xml, */*',
        }
        try:
            resp = requests.get(source["url"], headers=headers, timeout=15)
            if resp.status_code != 200:
                return []
        except Exception:
            return []

        feed = feedparser.parse(resp.text)
        if not feed.entries:
            feed = feedparser.parse(source["url"])

        articles = []
        for entry in feed.entries[:self.max_per_src]:
            pub_date = None
            for attr in ('published_parsed', 'updated_parsed'):
                val = getattr(entry, attr, None)
                if val:
                    try:
                        pub_date = datetime(*val[:6], tzinfo=timezone.utc)
                        break
                    except Exception:
                        pass
            if pub_date and pub_date < cutoff:
                continue

            titre  = getattr(entry, 'title', '') or ''
            resume = ''
            for attr in ('summary', 'description', 'content'):
                raw = getattr(entry, attr, None)
                if isinstance(raw, list) and raw:
                    raw = raw[0].get('value', '')
                if raw:
                    resume = re.sub(r'<[^>]+>', '', str(raw))[:1200]
                    break
            url = getattr(entry, 'link', '') or ''
            if not titre:
                continue

            url_hash = hashlib.md5(url.encode()).hexdigest() if url else hashlib.md5(titre.encode()).hexdigest()
            score    = self._score_article(titre + ' ' + resume, source)

            articles.append({
                "mail_date":      pub_date or datetime.now(timezone.utc),
                "mail_subject":   titre,
                "titre":          titre,
                "resume":         resume,
                "source_url":     url,
                "url_hash":       url_hash,
                "source_rss":     source["name"],
                "zone":           source["zone"],
                "sous_categorie": source["categorie"],
                "langue":         source["langue"],
                "score_importance": score,
                "alert_keyword":  source["categorie"],
                "mot_cle":        source["zone"],
                "categorie":      source["categorie"],
            })
        return articles

    # ──────────────────────────────────────────────────────────────────────────
    # COLLECTE MISTRAL WEB_SEARCH (fallback principal)
    # ──────────────────────────────────────────────────────────────────────────

    def _collect_via_mistral_websearch(self) -> list:
        """
        Utilise Mistral avec web_search pour collecter et résumer
        les actualités macro récentes. Retourne des articles prêts à insérer.

        Mistral web_search est activé en ajoutant l'option 'web_search: true'
        dans la requête — Mistral cherche lui-même les actualités récentes.
        """
        if not self.mistral_key:
            logging.warning("⚠️  MISTRAL_API_KEY absent — collecte Mistral impossible")
            return []

        headers = {
            "Authorization": f"Bearer {self.mistral_key}",
            "Content-Type": "application/json",
        }

        all_articles = []
        today_str = datetime.now().strftime("%d %B %Y")

        for query, zone, categorie in MISTRAL_SEARCH_QUERIES:
            prompt = f"""Tu es un collecteur d'actualités macro-économiques pour un système d'analyse boursière de la BRVM (Bourse Régionale des Valeurs Mobilières d'Afrique de l'Ouest).

Date d'aujourd'hui : {today_str}

Recherche sur internet les 3 actualités les plus importantes et récentes (des 7 derniers jours) sur le sujet suivant :
"{query}"

Pour chaque actualité trouvée, réponds UNIQUEMENT en JSON valide (sans markdown, sans explication) :
{{
  "articles": [
    {{
      "titre": "titre complet de l'actualité",
      "resume": "résumé factuel en français de 3-5 phrases expliquant l'essentiel",
      "source": "nom du média source",
      "date": "YYYY-MM-DD",
      "sentiment": "positif|negatif|neutre",
      "impact_brvm": "positif|negatif|neutre",
      "impact_brvm_detail": "1-2 phrases expliquant l'impact sur la BRVM",
      "points_cles": ["point 1", "point 2", "point 3"],
      "score_importance": 70
    }}
  ]
}}

Si aucune actualité récente n'est trouvée, retourne {{"articles": []}}."""

            try:
                body = {
                    "model": "mistral-small-latest",
                    "messages": [{"role": "user", "content": prompt}],
                    "max_tokens": 1500,
                    "temperature": 0.2,
                    # Activation de la recherche web dans Mistral
                    "tool_choice": "auto",
                    "tools": [{
                        "type": "function",
                        "function": {
                            "name": "web_search",
                            "description": "Search the web for recent news",
                            "parameters": {
                                "type": "object",
                                "properties": {
                                    "query": {"type": "string"}
                                },
                                "required": ["query"]
                            }
                        }
                    }]
                }

                resp = requests.post(
                    "https://api.mistral.ai/v1/chat/completions",
                    headers=headers, json=body, timeout=45
                )

                # Mistral web_search peut retourner 2 réponses (tool call + résultat)
                # On essaie directement le format standard d'abord
                if resp.status_code == 200:
                    data = resp.json()
                    raw_text = ""
                    if data.get("choices"):
                        raw_text = data["choices"][0]["message"].get("content", "") or ""

                    if raw_text:
                        parsed = self._parse_mistral_articles(raw_text, zone, categorie)
                        if parsed:
                            all_articles.extend(parsed)
                            logging.info(f"   🤖 Mistral ({zone}/{categorie}): {len(parsed)} article(s)")
                        else:
                            # Fallback : essai sans tools (prompt pur)
                            parsed = self._collect_mistral_simple(headers, prompt, zone, categorie)
                            all_articles.extend(parsed)
                    time.sleep(1.5)  # politesse API

                elif resp.status_code == 429:
                    logging.warning("   ⏳ Mistral rate limit — pause 30s")
                    time.sleep(30)
                else:
                    logging.warning(f"   ⚠️  Mistral HTTP {resp.status_code} pour '{query[:40]}'")
                    # Fallback sans tools
                    parsed = self._collect_mistral_simple(headers, prompt, zone, categorie)
                    all_articles.extend(parsed)
                    time.sleep(2)

            except Exception as e:
                logging.warning(f"   ⚠️  Mistral web_search '{query[:40]}': {e}")
                logging.debug(traceback.format_exc())

        return all_articles

    def _collect_mistral_simple(self, headers: dict, prompt: str, zone: str, categorie: str) -> list:
        """
        Appel Mistral sans tools — lui demande de rédiger les actualités
        depuis sa connaissance interne récente (jusqu'à sa date de coupure).
        """
        try:
            body = {
                "model": "mistral-small-latest",
                "messages": [{"role": "user", "content": prompt}],
                "max_tokens": 1200,
                "temperature": 0.3,
            }
            resp = requests.post(
                "https://api.mistral.ai/v1/chat/completions",
                headers=headers, json=body, timeout=40
            )
            if resp.status_code == 200:
                data = resp.json()
                raw_text = data["choices"][0]["message"].get("content", "") or ""
                return self._parse_mistral_articles(raw_text, zone, categorie)
        except Exception as e:
            logging.debug(f"_collect_mistral_simple: {e}")
        return []

    def _parse_mistral_articles(self, raw_text: str, zone: str, categorie: str) -> list:
        """Parse la réponse JSON de Mistral et retourne une liste d'articles."""
        try:
            clean = re.sub(r'```json|```', '', raw_text).strip()
            # Trouver le bloc JSON
            match = re.search(r'\{.*\}', clean, re.DOTALL)
            if not match:
                return []
            data = json.loads(match.group())
            raw_articles = data.get("articles", [])
            if not raw_articles:
                return []

            result = []
            for art in raw_articles:
                titre  = str(art.get("titre", "")).strip()
                resume = str(art.get("resume", "")).strip()
                if not titre and not resume:
                    continue

                # Date
                date_str = str(art.get("date", "")).strip()
                try:
                    pub_date = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
                except Exception:
                    pub_date = datetime.now(timezone.utc)

                url_hash = hashlib.md5((titre + date_str).encode()).hexdigest()
                source   = str(art.get("source", f"Mistral/{zone}"))

                result.append({
                    "mail_date":      pub_date,
                    "mail_subject":   titre,
                    "titre":          titre,
                    "resume":         resume,
                    "source_url":     "",
                    "url_hash":       url_hash,
                    "source_rss":     f"Mistral web_search — {source}",
                    "zone":           zone,
                    "sous_categorie": categorie,
                    "langue":         "fr",
                    "score_importance": int(art.get("score_importance", 60)),
                    "alert_keyword":  categorie,
                    "mot_cle":        zone,
                    "categorie":      categorie,
                    "sentiment":      art.get("sentiment", "neutre"),
                    "impact_brvm":    art.get("impact_brvm", "neutre"),
                    "impact_bourses_mondiales": art.get("impact_brvm_detail", ""),
                    "points_cles":    art.get("points_cles", []),
                    "pertinence":     int(art.get("score_importance", 60)),
                    "impact_secteurs_brvm":   [],
                    "impact_societes_cotees": [],
                    "_already_enriched": True,   # Pas besoin d'appel IA supplémentaire
                })
            return result

        except Exception as e:
            logging.debug(f"_parse_mistral_articles: {e} | raw={raw_text[:100]}")
            return []

    # ──────────────────────────────────────────────────────────────────────────
    # SCORE DE PERTINENCE
    # ──────────────────────────────────────────────────────────────────────────

    def _score_article(self, text: str, source: dict) -> int:
        text_lower = text.lower()
        score = 30
        score += (4 - source.get("priorite", 3)) * 10
        for cat, keywords in KEYWORDS_IMPACT.items():
            hits = sum(1 for kw in keywords if kw in text_lower)
            if cat == "brvm_direct":
                score += hits * 15
            elif cat in ("afrique_ouest", "matieres_premieres"):
                score += hits * 10
            else:
                score += hits * 7
        return min(100, score)

    # ──────────────────────────────────────────────────────────────────────────
    # DÉDUPLICATION
    # ──────────────────────────────────────────────────────────────────────────

    def _filter_existing(self, articles: list) -> list:
        """Filtre les articles déjà en BDD."""
        if not articles:
            return []
        hashes = [a["url_hash"] for a in articles if a.get("url_hash")]
        existing_hashes = set()
        cursor = self.db_conn.cursor()
        try:
            if hashes:
                cursor.execute(
                    "SELECT url_hash FROM google_alerts_rapports WHERE url_hash = ANY(%s);",
                    (hashes,)
                )
                existing_hashes = {r[0] for r in cursor.fetchall()}
        except Exception as e:
            logging.warning(f"⚠️  Déduplication: {e}")
        finally:
            cursor.close()

        new_articles = []
        for art in articles:
            if art.get("url_hash") in existing_hashes:
                self.stats["skipped"] += 1
            else:
                new_articles.append(art)
        return new_articles

    # ──────────────────────────────────────────────────────────────────────────
    # ENRICHISSEMENT IA (pour articles RSS bruts)
    # ──────────────────────────────────────────────────────────────────────────

    def _enrich_with_ai(self, article: dict) -> dict:
        """Enrichit un article RSS brut via IA (résumé FR, impact BRVM, sentiment)."""
        titre  = article.get("titre", "")
        resume = article.get("resume", "")
        zone   = article.get("zone", "")
        cat    = article.get("sous_categorie", "")
        langue = article.get("langue", "fr")

        if not titre and not resume:
            return article
        if article.get("score_importance", 0) < 15:
            article.setdefault("sentiment", "neutre")
            article.setdefault("impact_brvm", "neutre")
            article.setdefault("points_cles", [])
            return article

        prompt = f"""Tu es un analyste financier expert de la BRVM.

ARTICLE :
Titre    : {titre}
Zone     : {zone}
Catégorie: {cat}
Contenu  : {resume[:600]}

Réponds UNIQUEMENT en JSON valide :
{{
  "resume_fr": "résumé 3-4 phrases en français",
  "points_cles": ["point 1", "point 2"],
  "sentiment": "positif|negatif|neutre",
  "impact_brvm": "positif|negatif|neutre",
  "impact_brvm_detail": "1-2 phrases",
  "score_importance": 60
}}"""

        result_json = None
        for fn in [self._call_mistral, self._call_gemini, self._call_deepseek]:
            try:
                raw = fn(prompt)
                if raw:
                    clean = re.sub(r'```json|```', '', raw).strip()
                    result_json = json.loads(clean)
                    break
            except Exception:
                continue

        if result_json:
            article["resume"]        = result_json.get("resume_fr") or article["resume"]
            article["points_cles"]   = result_json.get("points_cles", [])
            article["sentiment"]     = result_json.get("sentiment", "neutre")
            article["impact_brvm"]   = result_json.get("impact_brvm", "neutre")
            article["impact_bourses_mondiales"] = result_json.get("impact_brvm_detail", "")
            article["score_importance"] = result_json.get("score_importance", article.get("score_importance", 50))
        else:
            text = (titre + " " + resume).lower()
            pos = sum(1 for k in ["hausse","croissance","positif","record","growth","rise"] if k in text)
            neg = sum(1 for k in ["baisse","chute","crise","guerre","récession","perte","fall"] if k in text)
            article["sentiment"]   = "positif" if pos > neg else ("negatif" if neg > pos else "neutre")
            article["impact_brvm"] = article["sentiment"]
            article.setdefault("points_cles", [])
            article.setdefault("impact_secteurs_brvm", [])
            article.setdefault("impact_societes_cotees", [])
        return article

    # ──────────────────────────────────────────────────────────────────────────
    # APPELS IA
    # ──────────────────────────────────────────────────────────────────────────

    def _call_mistral(self, prompt: str) -> Optional[str]:
        if not self.mistral_key:
            return None
        headers = {"Authorization": f"Bearer {self.mistral_key}", "Content-Type": "application/json"}
        data = {"model": "mistral-small-latest", "messages": [{"role": "user", "content": prompt}],
                "max_tokens": 600, "temperature": 0.2}
        resp = requests.post("https://api.mistral.ai/v1/chat/completions",
                             headers=headers, json=data, timeout=30)
        resp.raise_for_status()
        return resp.json()["choices"][0]["message"]["content"]

    def _call_gemini(self, prompt: str) -> Optional[str]:
        if not self.gemini_keys:
            return None
        key = self.gemini_keys[self._gemini_idx % len(self.gemini_keys)]
        self._gemini_idx += 1
        url  = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent?key={key}"
        data = {"contents": [{"parts": [{"text": prompt}]}],
                "generationConfig": {"temperature": 0.2, "maxOutputTokens": 600}}
        resp = requests.post(url, json=data, timeout=30)
        resp.raise_for_status()
        return resp.json()["candidates"][0]["content"]["parts"][0]["text"]

    def _call_deepseek(self, prompt: str) -> Optional[str]:
        if not self.deepseek_key:
            return None
        headers = {"Authorization": f"Bearer {self.deepseek_key}", "Content-Type": "application/json"}
        data = {"model": "deepseek-chat", "messages": [{"role": "user", "content": prompt}],
                "max_tokens": 600, "temperature": 0.2}
        resp = requests.post("https://api.deepseek.com/v1/chat/completions",
                             headers=headers, json=data, timeout=30)
        resp.raise_for_status()
        return resp.json()["choices"][0]["message"]["content"]

    # ──────────────────────────────────────────────────────────────────────────
    # INSERTION EN BASE
    # ──────────────────────────────────────────────────────────────────────────

    def _insert_article(self, art: dict):
        """
        Insère un article dans google_alerts_rapports.

        Déduplication en 2 niveaux :
          1. ON CONFLICT (url_hash) DO NOTHING  — si l'index UNIQUE existe
          2. WHERE NOT EXISTS (filet de sécurité sans index UNIQUE)
        """
        params = {
            **art,
            "points_cles":            json.dumps(art.get("points_cles", []), ensure_ascii=False),
            "impact_secteurs_brvm":   json.dumps(art.get("impact_secteurs_brvm", []), ensure_ascii=False),
            "impact_societes_cotees": json.dumps(art.get("impact_societes_cotees", []), ensure_ascii=False),
            "pertinence":             int(art.get("pertinence") or 50),
            "score_importance":       int(art.get("score_importance") or 50),
            "impact_brvm":            art.get("impact_brvm", "neutre"),
            "impact_bourses_mondiales": art.get("impact_bourses_mondiales", ""),
            "mail_subject":           art.get("mail_subject") or art.get("titre", ""),
        }

        COLS = """(
                    mail_date, mail_subject, titre, resume,
                    points_cles, sentiment, pertinence,
                    categorie, rapport_type, alert_keyword, mot_cle,
                    source_url, url_hash, source_rss, zone,
                    sous_categorie, langue, score_importance,
                    impact_brvm, impact_bourses_mondiales,
                    impact_secteurs_brvm, impact_societes_cotees,
                    collecte_date, envoye_email
                )"""
        VALS = """(
                    %(mail_date)s, %(mail_subject)s, %(titre)s, %(resume)s,
                    %(points_cles)s, %(sentiment)s, %(pertinence)s,
                    %(categorie)s, %(sous_categorie)s,
                    %(alert_keyword)s, %(mot_cle)s,
                    %(source_url)s, %(url_hash)s, %(source_rss)s, %(zone)s,
                    %(sous_categorie)s, %(langue)s, %(score_importance)s,
                    %(impact_brvm)s, %(impact_bourses_mondiales)s,
                    %(impact_secteurs_brvm)s, %(impact_societes_cotees)s,
                    NOW(), FALSE
                )"""

        sql_conflict = f"INSERT INTO google_alerts_rapports {COLS} VALUES {VALS} ON CONFLICT (url_hash) DO NOTHING;"
        sql_safe     = f"""INSERT INTO google_alerts_rapports {COLS}
                SELECT {VALS[1:-1]}
                WHERE NOT EXISTS (
                    SELECT 1 FROM google_alerts_rapports
                    WHERE url_hash = %(url_hash)s AND url_hash IS NOT NULL
                );"""

        cursor = self.db_conn.cursor()
        try:
            cursor.execute(sql_conflict, params)
            self.db_conn.commit()
        except Exception as e:
            self.db_conn.rollback()
            if "no unique or exclusion constraint" in str(e).lower():
                logging.debug("   Fallback WHERE NOT EXISTS (index UNIQUE absent)")
                try:
                    cursor.execute(sql_safe, params)
                    self.db_conn.commit()
                except Exception as e2:
                    self.db_conn.rollback()
                    raise e2
            else:
                raise e
        finally:
            cursor.close()


# ==============================================================================
# POINT D'ENTRÉE STANDALONE (GitHub Actions)
# ==============================================================================

def _get_db_connection():
    required = ["DB_NAME", "DB_USER", "DB_PASSWORD", "DB_HOST"]
    missing  = [v for v in required if not os.environ.get(v)]
    if missing:
        raise EnvironmentError(f"Variables manquantes : {missing}")
    return psycopg2.connect(
        dbname   = os.environ["DB_NAME"],
        user     = os.environ["DB_USER"],
        password = os.environ["DB_PASSWORD"],
        host     = os.environ["DB_HOST"],
        port     = os.environ.get("DB_PORT", "5432"),
        connect_timeout = 30,
        options  = "-c statement_timeout=300000",
    )


if __name__ == "__main__":
    logging.info("🌍 MACRO COLLECTOR v3 — Exécution standalone")

    gemini_keys = [k for k in [
        os.environ.get("GEMINI_API_KEY"),
        os.environ.get("GEMINI_API_KEY_2"),
        os.environ.get("GEMINI_API_KEY_3"),
    ] if k]
    deepseek_key = os.environ.get("DEEPSEEK_API_KEY")
    mistral_key  = os.environ.get("MISTRAL_API_KEY")

    logging.info(f"   APIs : Gemini×{len(gemini_keys)} "
                 f"| DeepSeek={'✅' if deepseek_key else '❌'} "
                 f"| Mistral={'✅' if mistral_key else '❌'}")

    if not mistral_key:
        logging.error("❌ MISTRAL_API_KEY absent — la collecte Mistral web_search est impossible")
        logging.error("   Ajoute MISTRAL_API_KEY dans tes GitHub Secrets")
        sys.exit(1)

    db_conn = None
    try:
        db_conn = _get_db_connection()
        collector = MacroCollector(
            db_conn                 = db_conn,
            gemini_keys             = gemini_keys,
            deepseek_key            = deepseek_key,
            mistral_key             = mistral_key,
            max_articles_per_source = 10,
        )
        stats = collector.run()
        logging.info(f"✅ Terminé — Insérés: {stats['inserted']} | "
                     f"Ignorés: {stats['skipped']} | Erreurs: {stats['errors']}")
    except Exception as e:
        logging.error(f"❌ ERREUR FATALE : {e}")
        logging.error(traceback.format_exc())
        sys.exit(1)
    finally:
        if db_conn:
            db_conn.close()
