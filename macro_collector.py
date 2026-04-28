# ==============================================================================
# macro_collector.py — Collecteur d'actualités macro-économiques mondiales
# Alimente google_alerts_rapports avec des actualités fraîches depuis des
# flux RSS couvrant : International / Afrique / UEMOA / Marchés financiers /
# Matières premières / Géopolitique / Politique monétaire
#
# CORRECTIONS v2 :
#   - Sources RSS remplacées par Google News RSS (seules URLs stables depuis GitHub Actions)
#   - cutoff étendu à 7 jours (au lieu de 3)
#   - Seuil de score IA abaissé à 15 (au lieu de 25)
#   - Stacktrace complète loggée en cas d'erreur
#   - __main__ standalone pour exécution depuis le workflow GitHub Actions
#   - _ensure_table_exists() crée la table si elle n'existe pas encore
#
# Intégration dans main.py :
#   from macro_collector import MacroCollector
#   MacroCollector(db_conn, gemini_keys, deepseek_key, mistral_key).run()
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
# SOURCES RSS — Google News uniquement (seules URLs fiables depuis GitHub Actions)
# Les sources directes (Reuters, BBC, RFI, etc.) retournent 403 sur les runners
# GitHub Actions car ils bloquent les user-agents de datacenter.
# Google News RSS est un agrégateur public qui fonctionne sans restriction.
# ==============================================================================
RSS_SOURCES = [

    # ── BRVM / UEMOA ────────────────────────────────────────────────────────
    {
        "name":      "Google News — BRVM bourse",
        "url":       "https://news.google.com/rss/search?q=BRVM+bourse+régionale&hl=fr&gl=CI&ceid=CI:fr",
        "zone":      "uemoa",
        "categorie": "marche_financier",
        "langue":    "fr",
        "priorite":  1,
    },
    {
        "name":      "Google News — BCEAO politique monétaire",
        "url":       "https://news.google.com/rss/search?q=BCEAO+politique+monétaire&hl=fr&gl=FR&ceid=FR:fr",
        "zone":      "uemoa",
        "categorie": "politique_monetaire",
        "langue":    "fr",
        "priorite":  1,
    },
    {
        "name":      "Google News — UEMOA économie",
        "url":       "https://news.google.com/rss/search?q=UEMOA+économie+croissance&hl=fr&gl=FR&ceid=FR:fr",
        "zone":      "uemoa",
        "categorie": "économique",
        "langue":    "fr",
        "priorite":  1,
    },
    {
        "name":      "Google News — Côte d'Ivoire économie",
        "url":       "https://news.google.com/rss/search?q=%22Côte+d%27Ivoire%22+économie+investissement&hl=fr&gl=CI&ceid=CI:fr",
        "zone":      "uemoa",
        "categorie": "économique",
        "langue":    "fr",
        "priorite":  1,
    },
    {
        "name":      "Google News — franc CFA",
        "url":       "https://news.google.com/rss/search?q=franc+CFA+FCFA+zone+franc&hl=fr&gl=FR&ceid=FR:fr",
        "zone":      "uemoa",
        "categorie": "politique_monetaire",
        "langue":    "fr",
        "priorite":  1,
    },

    # ── AFRIQUE ─────────────────────────────────────────────────────────────
    {
        "name":      "Google News — Afrique économie",
        "url":       "https://news.google.com/rss/search?q=afrique+économie+croissance+investissement&hl=fr&gl=FR&ceid=FR:fr",
        "zone":      "afrique",
        "categorie": "économique",
        "langue":    "fr",
        "priorite":  1,
    },
    {
        "name":      "Google News — Afrique de l'Ouest",
        "url":       "https://news.google.com/rss/search?q=%22Afrique+de+l%27Ouest%22+OR+CEDEAO+économie&hl=fr&gl=FR&ceid=FR:fr",
        "zone":      "afrique",
        "categorie": "économique",
        "langue":    "fr",
        "priorite":  1,
    },
    {
        "name":      "Google News — Sénégal Mali Burkina",
        "url":       "https://news.google.com/rss/search?q=Sénégal+OR+Mali+OR+%22Burkina+Faso%22+économie&hl=fr&gl=FR&ceid=FR:fr",
        "zone":      "afrique",
        "categorie": "économique",
        "langue":    "fr",
        "priorite":  2,
    },
    {
        "name":      "Google News — Nigeria Ghana économie",
        "url":       "https://news.google.com/rss/search?q=Nigeria+OR+Ghana+economy+finance&hl=en&gl=NG&ceid=NG:en",
        "zone":      "afrique",
        "categorie": "économique",
        "langue":    "en",
        "priorite":  2,
    },
    {
        "name":      "Google News — dette Afrique FMI",
        "url":       "https://news.google.com/rss/search?q=Afrique+dette+FMI+%22Banque+Mondiale%22&hl=fr&gl=FR&ceid=FR:fr",
        "zone":      "afrique",
        "categorie": "économique",
        "langue":    "fr",
        "priorite":  2,
    },

    # ── MATIÈRES PREMIÈRES (crucial pour la BRVM) ───────────────────────────
    {
        "name":      "Google News — cacao prix marché",
        "url":       "https://news.google.com/rss/search?q=cacao+prix+marché+cocoa&hl=fr&gl=FR&ceid=FR:fr",
        "zone":      "uemoa",
        "categorie": "matieres_premieres",
        "langue":    "fr",
        "priorite":  1,
    },
    {
        "name":      "Google News — pétrole or matières premières",
        "url":       "https://news.google.com/rss/search?q=pétrole+prix+OR+%22cours+de+l%27or%22+OR+%22matières+premières%22&hl=fr&gl=FR&ceid=FR:fr",
        "zone":      "international",
        "categorie": "matieres_premieres",
        "langue":    "fr",
        "priorite":  1,
    },
    {
        "name":      "Google News — caoutchouc coton palmier",
        "url":       "https://news.google.com/rss/search?q=caoutchouc+OR+coton+OR+%22huile+de+palme%22+prix&hl=fr&gl=FR&ceid=FR:fr",
        "zone":      "uemoa",
        "categorie": "matieres_premieres",
        "langue":    "fr",
        "priorite":  1,
    },
    {
        "name":      "Google News — commodities markets",
        "url":       "https://news.google.com/rss/search?q=commodities+markets+cocoa+palm+oil&hl=en&gl=US&ceid=US:en",
        "zone":      "international",
        "categorie": "matieres_premieres",
        "langue":    "en",
        "priorite":  2,
    },

    # ── MARCHÉS FINANCIERS INTERNATIONAUX ───────────────────────────────────
    {
        "name":      "Google News — Fed taux inflation",
        "url":       "https://news.google.com/rss/search?q=%22Federal+Reserve%22+OR+%22taux+directeur%22+inflation&hl=fr&gl=FR&ceid=FR:fr",
        "zone":      "international",
        "categorie": "politique_monetaire",
        "langue":    "fr",
        "priorite":  1,
    },
    {
        "name":      "Google News — BCE taux euro",
        "url":       "https://news.google.com/rss/search?q=BCE+%22banque+centrale+européenne%22+taux&hl=fr&gl=FR&ceid=FR:fr",
        "zone":      "international",
        "categorie": "politique_monetaire",
        "langue":    "fr",
        "priorite":  1,
    },
    {
        "name":      "Google News — marchés financiers bourse",
        "url":       "https://news.google.com/rss/search?q=marchés+financiers+bourse+indices&hl=fr&gl=FR&ceid=FR:fr",
        "zone":      "international",
        "categorie": "marche_financier",
        "langue":    "fr",
        "priorite":  2,
    },
    {
        "name":      "Google News — dollar euro change",
        "url":       "https://news.google.com/rss/search?q=dollar+euro+taux+change+forex&hl=fr&gl=FR&ceid=FR:fr",
        "zone":      "international",
        "categorie": "marche_financier",
        "langue":    "fr",
        "priorite":  2,
    },
    {
        "name":      "Google News — recession inflation économie mondiale",
        "url":       "https://news.google.com/rss/search?q=récession+OR+inflation+%22économie+mondiale%22&hl=fr&gl=FR&ceid=FR:fr",
        "zone":      "international",
        "categorie": "économique",
        "langue":    "fr",
        "priorite":  1,
    },

    # ── GÉOPOLITIQUE ────────────────────────────────────────────────────────
    {
        "name":      "Google News — géopolitique Afrique",
        "url":       "https://news.google.com/rss/search?q=géopolitique+Afrique+sanctions+instabilité&hl=fr&gl=FR&ceid=FR:fr",
        "zone":      "afrique",
        "categorie": "geopolitique",
        "langue":    "fr",
        "priorite":  2,
    },
    {
        "name":      "Google News — FMI Banque Mondiale Afrique",
        "url":       "https://news.google.com/rss/search?q=FMI+%22Banque+Mondiale%22+Afrique+prêt&hl=fr&gl=FR&ceid=FR:fr",
        "zone":      "afrique",
        "categorie": "économique",
        "langue":    "fr",
        "priorite":  1,
    },
]

# ==============================================================================
# MOTS-CLÉS PAR CATÉGORIE pour le scoring de pertinence
# ==============================================================================
KEYWORDS_IMPACT = {
    "brvm_direct": [
        "brvm", "bourse abidjan", "côte d'ivoire", "bourse régionale",
        "uemoa", "fcfa", "bceao", "zone franc", "abidjan",
    ],
    "afrique_ouest": [
        "sénégal", "mali", "burkina", "niger", "togo", "bénin", "ghana",
        "nigeria", "cedeao", "ecowas", "afrique de l'ouest", "west africa",
    ],
    "matieres_premieres": [
        "cacao", "cocoa", "caoutchouc", "rubber", "pétrole", "oil", "gas",
        "or", "gold", "coton", "cotton", "café", "coffee", "anacarde",
        "cashew", "palmier", "palm oil", "matières premières", "commodities",
    ],
    "marches_financiers": [
        "wall street", "dow jones", "s&p500", "sp500", "nasdaq", "euronext",
        "cac40", "dax", "nikkei", "bourse", "indice", "stock market",
        "taux d'intérêt", "interest rate", "fed", "federal reserve",
        "bce", "banque centrale", "inflation", "récession", "recession",
    ],
    "geopolitique": [
        "guerre", "war", "conflit", "conflict", "sanctions", "embargo",
        "ukraine", "russie", "russia", "moyen-orient", "middle east",
        "coup d'état", "putsch", "transition", "élection", "election",
        "dette souveraine", "sovereign debt", "fmi", "imf",
    ],
}

# ==============================================================================
# CLASSE PRINCIPALE
# ==============================================================================

class MacroCollector:
    """
    Collecte les actualités macro-économiques depuis des flux Google News RSS,
    analyse leur impact via IA et insère dans google_alerts_rapports.
    """

    def __init__(self, db_conn, gemini_keys: list, deepseek_key: str,
                 mistral_key: str, max_articles_per_source: int = 8):
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
        """Lance la collecte complète."""
        logging.info("="*60)
        logging.info("🌍 MACRO COLLECTOR — Démarrage collecte RSS Google News")
        logging.info("="*60)

        # Créer la table si elle n'existe pas, puis vérifier les colonnes
        self._ensure_table_exists()
        self._ensure_table_columns()

        articles = self._fetch_all_rss()
        logging.info(f"📥 {len(articles)} article(s) collectés depuis les flux RSS")

        if not articles:
            logging.warning("⚠️  Aucun article collecté — vérifier la connectivité réseau")
            return self.stats

        # Filtrer les doublons déjà en BDD
        articles = self._filter_existing(articles)
        logging.info(f"🔍 {len(articles)} article(s) nouveaux après déduplication")

        if not articles:
            logging.info("✅ Aucun nouvel article — BDD déjà à jour")
            return self.stats

        # Analyser et insérer par lots de 10
        batch_size = 10
        for i in range(0, len(articles), batch_size):
            batch = articles[i:i + batch_size]
            for art in batch:
                try:
                    enriched = self._enrich_with_ai(art)
                    self._insert_article(enriched)
                    self.stats["inserted"] += 1
                    time.sleep(0.5)   # politesse API
                except Exception as e:
                    logging.error(f"❌ Erreur insertion {art.get('titre','?')[:50]}: {e}")
                    logging.debug(traceback.format_exc())
                    self.stats["errors"] += 1

        logging.info("="*60)
        logging.info(f"✅ Collecte terminée — Insérés: {self.stats['inserted']} | "
                     f"Ignorés: {self.stats['skipped']} | Erreurs: {self.stats['errors']}")
        logging.info("="*60)
        return self.stats

    # ──────────────────────────────────────────────────────────────────────────
    # CRÉATION / VÉRIFICATION DE LA TABLE ET DES COLONNES
    # ──────────────────────────────────────────────────────────────────────────

    def _ensure_table_exists(self):
        """
        Crée la table google_alerts_rapports si elle n'existe pas.
        Inclut toutes les colonnes dès la création.
        """
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
            # Index utiles
            for idx_sql in [
                "CREATE INDEX IF NOT EXISTS idx_gar_url_hash      ON google_alerts_rapports(url_hash);",
                "CREATE INDEX IF NOT EXISTS idx_gar_zone          ON google_alerts_rapports(zone);",
                "CREATE INDEX IF NOT EXISTS idx_gar_impact_brvm   ON google_alerts_rapports(impact_brvm);",
                "CREATE INDEX IF NOT EXISTS idx_gar_score         ON google_alerts_rapports(score_importance DESC);",
                "CREATE INDEX IF NOT EXISTS idx_gar_collecte_date ON google_alerts_rapports(collecte_date DESC);",
            ]:
                cursor.execute(idx_sql)
            self.db_conn.commit()
            logging.info("✅ Table google_alerts_rapports prête")
        except Exception as e:
            self.db_conn.rollback()
            logging.warning(f"⚠️  _ensure_table_exists: {e}")
            logging.debug(traceback.format_exc())
        finally:
            cursor.close()

    def _ensure_table_columns(self):
        """
        Vérifie et ajoute les colonnes manquantes (migration idempotente).
        Utile si la table existait déjà sans certaines colonnes.
        """
        colonnes_requises = [
            ("zone",                       "TEXT"),
            ("sous_categorie",             "TEXT"),
            ("impact_brvm",                "TEXT DEFAULT 'neutre'"),
            ("impact_bourses_mondiales",   "TEXT"),
            ("impact_secteurs_brvm",       "JSONB"),
            ("impact_societes_cotees",     "JSONB"),
            ("source_rss",                 "TEXT"),
            ("collecte_date",              "TIMESTAMPTZ DEFAULT NOW()"),
            ("langue",                     "TEXT DEFAULT 'fr'"),
            ("score_importance",           "INTEGER DEFAULT 50"),
            ("url_hash",                   "TEXT"),
        ]

        cursor = self.db_conn.cursor()
        for col_name, col_type in colonnes_requises:
            try:
                cursor.execute(f"""
                    ALTER TABLE google_alerts_rapports
                    ADD COLUMN IF NOT EXISTS {col_name} {col_type};
                """)
                self.db_conn.commit()
            except Exception as e:
                self.db_conn.rollback()
                logging.debug(f"Colonne {col_name} déjà présente ou erreur: {e}")
        cursor.close()
        logging.info("✅ Colonnes google_alerts_rapports vérifiées")

    # ──────────────────────────────────────────────────────────────────────────
    # COLLECTE RSS
    # ──────────────────────────────────────────────────────────────────────────

    def _fetch_all_rss(self) -> list:
        """Parcourt toutes les sources RSS et retourne une liste d'articles bruts."""
        all_articles = []
        # CORRECTION : 7 jours au lieu de 3 pour ne pas rater des articles
        cutoff = datetime.now(timezone.utc) - timedelta(days=7)

        for source in RSS_SOURCES:
            try:
                articles = self._fetch_one_rss(source, cutoff)
                all_articles.extend(articles)
                self.stats["fetched"] += len(articles)
                logging.info(
                    f"   📡 {source['name']:<45} → {len(articles)} article(s)"
                )
                time.sleep(0.5)   # politesse serveur
            except Exception as e:
                logging.warning(f"   ⚠️  {source['name']}: {e}")
                self.stats["errors"] += 1

        return all_articles

    def _fetch_one_rss(self, source: dict, cutoff: datetime) -> list:
        """Collecte les articles d'une source RSS unique."""
        headers = {
            # User-agent neutre — Google News RSS tolère les requêtes simples
            'User-Agent': 'Mozilla/5.0 (compatible; Feedfetcher-Google; +http://www.google.com/feedfetcher.html)',
            'Accept': 'application/rss+xml, application/xml, text/xml, */*',
            'Accept-Language': 'fr-FR,fr;q=0.9,en;q=0.8',
        }

        try:
            resp = requests.get(source["url"], headers=headers, timeout=20)
            resp.raise_for_status()
        except requests.exceptions.HTTPError as e:
            logging.debug(f"HTTP {e.response.status_code} pour {source['name']}")
            return []
        except Exception as e:
            logging.debug(f"Réseau pour {source['name']}: {e}")
            return []

        feed = feedparser.parse(resp.text)
        if not feed.entries:
            # Tentative directe via feedparser (parfois contourne le 403)
            feed = feedparser.parse(source["url"])

        articles = []
        for entry in feed.entries[:self.max_per_src]:
            # Date de publication
            pub_date = None
            for attr in ('published_parsed', 'updated_parsed'):
                val = getattr(entry, attr, None)
                if val:
                    try:
                        pub_date = datetime(*val[:6], tzinfo=timezone.utc)
                        break
                    except Exception:
                        pass

            # Filtrer les articles trop anciens
            if pub_date and pub_date < cutoff:
                continue

            # Contenu
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

            if not titre and not resume:
                continue

            # Hash URL pour déduplication
            url_hash = hashlib.md5(url.encode()).hexdigest() if url else \
                       hashlib.md5(titre.encode()).hexdigest()

            # Score de pertinence préliminaire
            score = self._score_article(titre + ' ' + resume, source)

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

    def _score_article(self, text: str, source: dict) -> int:
        """Score de pertinence 0-100 basé sur les mots-clés d'impact."""
        text_lower = text.lower()
        score = 30   # base

        # Bonus priorité source
        score += (4 - source.get("priorite", 3)) * 10

        # Bonus mots-clés
        for cat, keywords in KEYWORDS_IMPACT.items():
            hits = sum(1 for kw in keywords if kw in text_lower)
            if cat == "brvm_direct":
                score += hits * 15
            elif cat in ("afrique_ouest", "matieres_premieres"):
                score += hits * 10
            elif cat in ("marches_financiers", "geopolitique"):
                score += hits * 7

        return min(100, score)

    # ──────────────────────────────────────────────────────────────────────────
    # DÉDUPLICATION
    # ──────────────────────────────────────────────────────────────────────────

    def _filter_existing(self, articles: list) -> list:
        """Supprime les articles déjà présents en BDD (par url_hash ou source_url)."""
        if not articles:
            return []

        hashes = [a["url_hash"] for a in articles if a.get("url_hash")]
        urls   = [a["source_url"] for a in articles if a.get("source_url")]

        existing_hashes = set()
        existing_urls   = set()

        cursor = self.db_conn.cursor()
        try:
            if hashes:
                cursor.execute(
                    "SELECT url_hash FROM google_alerts_rapports "
                    "WHERE url_hash = ANY(%s);",
                    (hashes,)
                )
                existing_hashes = {r[0] for r in cursor.fetchall()}
            if urls:
                cursor.execute(
                    "SELECT source_url FROM google_alerts_rapports "
                    "WHERE source_url = ANY(%s);",
                    (urls,)
                )
                existing_urls = {r[0] for r in cursor.fetchall()}
        except Exception as e:
            logging.warning(f"⚠️  Déduplication: {e}")
        finally:
            cursor.close()

        new_articles = []
        for art in articles:
            if art.get("url_hash") in existing_hashes:
                self.stats["skipped"] += 1
                continue
            if art.get("source_url") in existing_urls:
                self.stats["skipped"] += 1
                continue
            new_articles.append(art)

        return new_articles

    # ──────────────────────────────────────────────────────────────────────────
    # ENRICHISSEMENT IA
    # ──────────────────────────────────────────────────────────────────────────

    def _enrich_with_ai(self, article: dict) -> dict:
        """
        Envoie l'article à une IA pour extraire :
        - Résumé enrichi en français
        - Sentiment (positif/négatif/neutre)
        - Impact sur la BRVM
        - Points clés
        """
        titre  = article.get("titre", "")
        resume = article.get("resume", "")
        zone   = article.get("zone", "")
        cat    = article.get("sous_categorie", "")
        langue = article.get("langue", "fr")

        if not titre and not resume:
            return article

        # CORRECTION : seuil abaissé à 15 (était 25, trop restrictif)
        if article.get("score_importance", 0) < 15:
            article["sentiment"]   = "neutre"
            article["impact_brvm"] = "neutre"
            article["points_cles"] = []
            return article

        prompt = f"""Tu es un analyste financier expert de la BRVM (Bourse Régionale des Valeurs Mobilières d'Afrique de l'Ouest).

ARTICLE À ANALYSER :
Titre    : {titre}
Zone     : {zone}
Catégorie: {cat}
Langue   : {langue}
Contenu  : {resume[:800]}

SOCIÉTÉS COTÉES BRVM (référence) :
Banques : SGBC, BICB, BICC, BOAB, BOABF, BOAC, BOAM, BOAN, BNTS, CBIB, ECOC, NSBC
Industrie : ABJC, CABC, NTLC, SICC, SPHC, SIVC, STAC, UNLC, SLBC
Énergie/Distrib : ETIT, SHEC, TTLC, TTLS, CIEC
Agro : PALC, SOGC, LNBB, CFAC
Télécom : ONTBF, SNTS

MISSION — Réponds UNIQUEMENT en JSON valide, sans markdown :
{{
  "resume_fr": "résumé en français 3-5 phrases, factuel et précis",
  "points_cles": ["point 1", "point 2", "point 3"],
  "sentiment": "positif|negatif|neutre",
  "pertinence": <entier 0-100>,
  "impact_brvm": "positif|negatif|neutre",
  "impact_brvm_detail": "explication 2-3 phrases de l'impact sur la BRVM",
  "impact_bourses_mondiales": "explication 1-2 phrases",
  "impact_secteurs_brvm": [
    {{"secteur": "Banques", "impact": "positif|negatif|neutre", "justification": "phrase courte"}},
    {{"secteur": "Matières premières", "impact": "positif|negatif|neutre", "justification": "phrase courte"}}
  ],
  "impact_societes_cotees": [
    {{"symbole": "SGBC", "impact": "positif|negatif|neutre", "raison": "phrase courte"}}
  ],
  "score_importance": <entier 0-100>
}}
Ne cite que les sociétés réellement concernées (max 5)."""

        # Tentative avec rotation des IA
        result_json = None
        for fn in [self._call_gemini, self._call_deepseek, self._call_mistral]:
            try:
                raw = fn(prompt)
                if raw:
                    clean = re.sub(r'```json|```', '', raw).strip()
                    result_json = json.loads(clean)
                    break
            except Exception as e:
                logging.debug(f"IA enrichissement: {e}")
                continue

        if result_json:
            article["resume"]                   = result_json.get("resume_fr") or article["resume"]
            article["points_cles"]              = result_json.get("points_cles", [])
            article["sentiment"]                = result_json.get("sentiment", "neutre")
            article["pertinence"]               = result_json.get("pertinence", 50)
            article["impact_brvm"]              = result_json.get("impact_brvm", "neutre")
            article["impact_bourses_mondiales"] = result_json.get("impact_bourses_mondiales", "")
            article["impact_secteurs_brvm"]     = result_json.get("impact_secteurs_brvm", [])
            article["impact_societes_cotees"]   = result_json.get("impact_societes_cotees", [])
            article["score_importance"]         = result_json.get("score_importance",
                                                                   article.get("score_importance", 50))
        else:
            # Fallback sans IA : sentiment par mots-clés simples
            text = (titre + " " + resume).lower()
            pos_kw = ["hausse", "croissance", "positif", "bénéfice", "accord",
                      "investissement", "record", "stabilité", "growth", "rise", "gain"]
            neg_kw = ["baisse", "chute", "crise", "conflit", "guerre", "sanction",
                      "récession", "déficit", "perte", "fall", "drop", "war", "risque"]
            pos_hits = sum(1 for k in pos_kw if k in text)
            neg_hits = sum(1 for k in neg_kw if k in text)
            article["sentiment"]  = "positif" if pos_hits > neg_hits else \
                                    "negatif" if neg_hits > pos_hits else "neutre"
            article["impact_brvm"]              = article["sentiment"]
            article["points_cles"]              = []
            article["impact_secteurs_brvm"]     = []
            article["impact_societes_cotees"]   = []

        return article

    # ──────────────────────────────────────────────────────────────────────────
    # APPELS IA
    # ──────────────────────────────────────────────────────────────────────────

    def _call_gemini(self, prompt: str) -> Optional[str]:
        if not self.gemini_keys:
            return None
        key = self.gemini_keys[self._gemini_idx % len(self.gemini_keys)]
        self._gemini_idx += 1
        url  = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent?key={key}"
        data = {"contents": [{"parts": [{"text": prompt}]}],
                "generationConfig": {"temperature": 0.2, "maxOutputTokens": 800}}
        resp = requests.post(url, json=data, timeout=30)
        resp.raise_for_status()
        return resp.json()["candidates"][0]["content"]["parts"][0]["text"]

    def _call_deepseek(self, prompt: str) -> Optional[str]:
        if not self.deepseek_key:
            return None
        headers = {"Authorization": f"Bearer {self.deepseek_key}",
                   "Content-Type": "application/json"}
        data = {"model": "deepseek-chat",
                "messages": [{"role": "user", "content": prompt}],
                "max_tokens": 800, "temperature": 0.2}
        resp = requests.post("https://api.deepseek.com/v1/chat/completions",
                             headers=headers, json=data, timeout=30)
        resp.raise_for_status()
        return resp.json()["choices"][0]["message"]["content"]

    def _call_mistral(self, prompt: str) -> Optional[str]:
        if not self.mistral_key:
            return None
        headers = {"Authorization": f"Bearer {self.mistral_key}",
                   "Content-Type": "application/json"}
        data = {"model": "mistral-small-latest",
                "messages": [{"role": "user", "content": prompt}],
                "max_tokens": 800, "temperature": 0.2}
        resp = requests.post("https://api.mistral.ai/v1/chat/completions",
                             headers=headers, json=data, timeout=30)
        resp.raise_for_status()
        return resp.json()["choices"][0]["message"]["content"]

    # ──────────────────────────────────────────────────────────────────────────
    # INSERTION EN BASE
    # ──────────────────────────────────────────────────────────────────────────

    def _insert_article(self, art: dict):
        """Insère un article enrichi dans google_alerts_rapports."""
        cursor = self.db_conn.cursor()
        try:
            cursor.execute("""
                INSERT INTO google_alerts_rapports (
                    mail_date, mail_subject, titre, resume,
                    points_cles, sentiment, pertinence,
                    categorie, rapport_type, alert_keyword, mot_cle,
                    source_url, url_hash, source_rss, zone,
                    sous_categorie, langue, score_importance,
                    impact_brvm, impact_bourses_mondiales,
                    impact_secteurs_brvm, impact_societes_cotees,
                    collecte_date, envoye_email
                ) VALUES (
                    %(mail_date)s, %(mail_subject)s, %(titre)s, %(resume)s,
                    %(points_cles)s, %(sentiment)s, %(pertinence)s,
                    %(categorie)s, %(sous_categorie)s,
                    %(alert_keyword)s, %(mot_cle)s,
                    %(source_url)s, %(url_hash)s, %(source_rss)s, %(zone)s,
                    %(sous_categorie)s, %(langue)s, %(score_importance)s,
                    %(impact_brvm)s, %(impact_bourses_mondiales)s,
                    %(impact_secteurs_brvm)s, %(impact_societes_cotees)s,
                    NOW(), FALSE
                )
                ON CONFLICT (url_hash) DO NOTHING;
            """, {
                **art,
                "points_cles":            json.dumps(art.get("points_cles", []),
                                                     ensure_ascii=False),
                "impact_secteurs_brvm":   json.dumps(art.get("impact_secteurs_brvm", []),
                                                     ensure_ascii=False),
                "impact_societes_cotees": json.dumps(art.get("impact_societes_cotees", []),
                                                     ensure_ascii=False),
                "pertinence":             int(art.get("pertinence") or 50),
                "score_importance":       int(art.get("score_importance") or 50),
                "impact_brvm":            art.get("impact_brvm", "neutre"),
                "impact_bourses_mondiales": art.get("impact_bourses_mondiales", ""),
                "mail_subject":           art.get("mail_subject") or art.get("titre", ""),
            })
            self.db_conn.commit()
        except Exception as e:
            self.db_conn.rollback()
            raise e
        finally:
            cursor.close()


# ==============================================================================
# POINT D'ENTRÉE STANDALONE
# Permet d'exécuter macro_collector.py directement depuis le workflow GitHub
# ==============================================================================

def _get_db_connection():
    """Crée une connexion PostgreSQL depuis les variables d'environnement."""
    required = ["DB_NAME", "DB_USER", "DB_PASSWORD", "DB_HOST"]
    missing  = [v for v in required if not os.environ.get(v)]
    if missing:
        raise EnvironmentError(f"Variables d'environnement manquantes : {missing}")
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
    logging.info("🌍 MACRO COLLECTOR — Exécution standalone")

    # Clés IA depuis l'environnement
    gemini_keys = [
        k for k in [
            os.environ.get("GEMINI_API_KEY"),
            os.environ.get("GEMINI_API_KEY_2"),
            os.environ.get("GEMINI_API_KEY_3"),
        ]
        if k
    ]
    deepseek_key = os.environ.get("DEEPSEEK_API_KEY")
    mistral_key  = os.environ.get("MISTRAL_API_KEY")

    logging.info(f"   APIs disponibles : Gemini×{len(gemini_keys)} "
                 f"| DeepSeek={'✅' if deepseek_key else '❌'} "
                 f"| Mistral={'✅' if mistral_key else '❌'}")

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
        logging.error(f"❌ ERREUR FATALE macro_collector : {e}")
        logging.error(traceback.format_exc())
        sys.exit(1)
    finally:
        if db_conn:
            db_conn.close()
