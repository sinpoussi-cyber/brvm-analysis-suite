# ==============================================================================
# macro_collector.py — Collecteur d'actualités macro-économiques mondiales
# Alimente google_alerts_rapports avec des actualités fraîches depuis des
# flux RSS couvrant : International / Afrique / UEMOA / Marchés financiers /
# Matières premières / Géopolitique / Politique monétaire
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
import hashlib
from datetime import datetime, timezone, timedelta
from typing import Optional

import psycopg2
import psycopg2.extras

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# ==============================================================================
# LISTE COMPLÈTE DES FLUX RSS
# Organisés par thème pour le filtrage et la catégorisation automatique
# ==============================================================================
RSS_SOURCES = [

    # ── AFRIQUE DE L'OUEST / UEMOA ──────────────────────────────────────────
    {
        "name":      "Agence Ecofin — Économie UEMOA",
        "url":       "https://www.agenceecofin.com/rss",
        "zone":      "uemoa",
        "categorie": "économique",
        "langue":    "fr",
        "priorite":  1,
    },
    {
        "name":      "Agence Ecofin — Finances",
        "url":       "https://www.agenceecofin.com/finances/rss",
        "zone":      "uemoa",
        "categorie": "financier",
        "langue":    "fr",
        "priorite":  1,
    },
    {
        "name":      "Agence Ecofin — Agro",
        "url":       "https://www.agenceecofin.com/gestion-publique/rss",
        "zone":      "afrique",
        "categorie": "économique",
        "langue":    "fr",
        "priorite":  2,
    },
    {
        "name":      "BCEAO — Communiqués officiels",
        "url":       "https://www.bceao.int/fr/rss.xml",
        "zone":      "uemoa",
        "categorie": "politique_monetaire",
        "langue":    "fr",
        "priorite":  1,
    },
    {
        "name":      "BRVM — Actualités bourse",
        "url":       "https://www.brvm.org/fr/rss.xml",
        "zone":      "uemoa",
        "categorie": "marche_financier",
        "langue":    "fr",
        "priorite":  1,
    },
    {
        "name":      "RFI Afrique — Actualités",
        "url":       "https://www.rfi.fr/fr/afrique/rss",
        "zone":      "afrique",
        "categorie": "general",
        "langue":    "fr",
        "priorite":  2,
    },
    {
        "name":      "RFI — Économie",
        "url":       "https://www.rfi.fr/fr/economie/rss",
        "zone":      "afrique",
        "categorie": "économique",
        "langue":    "fr",
        "priorite":  2,
    },
    {
        "name":      "Le Monde Afrique",
        "url":       "https://www.lemonde.fr/afrique/rss_full.xml",
        "zone":      "afrique",
        "categorie": "general",
        "langue":    "fr",
        "priorite":  2,
    },
    {
        "name":      "Jeune Afrique — Économie",
        "url":       "https://www.jeuneafrique.com/feed",
        "zone":      "afrique",
        "categorie": "économique",
        "langue":    "fr",
        "priorite":  1,
    },
    {
        "name":      "The Africa Report",
        "url":       "https://www.theafricareport.com/feed/",
        "zone":      "afrique",
        "categorie": "économique",
        "langue":    "en",
        "priorite":  2,
    },
    {
        "name":      "African Business Magazine",
        "url":       "https://african.business/feed",
        "zone":      "afrique",
        "categorie": "économique",
        "langue":    "en",
        "priorite":  2,
    },
    {
        "name":      "Africanews — Économie",
        "url":       "https://www.africanews.com/feed/",
        "zone":      "afrique",
        "categorie": "general",
        "langue":    "en",
        "priorite":  3,
    },
    {
        "name":      "ONU Afrique — Actualités",
        "url":       "https://news.un.org/feed/subscribe/fr/news/region/africa/feed/rss.xml",
        "zone":      "afrique",
        "categorie": "geopolitique",
        "langue":    "fr",
        "priorite":  3,
    },

    # ── INTERNATIONAL FINANCE & MARCHÉS ────────────────────────────────────
    {
        "name":      "Reuters — Top News",
        "url":       "https://feeds.reuters.com/reuters/topNews",
        "zone":      "international",
        "categorie": "general",
        "langue":    "en",
        "priorite":  1,
    },
    {
        "name":      "Reuters — Business",
        "url":       "https://feeds.reuters.com/reuters/businessNews",
        "zone":      "international",
        "categorie": "économique",
        "langue":    "en",
        "priorite":  1,
    },
    {
        "name":      "Reuters — Africa",
        "url":       "https://feeds.reuters.com/reuters/AFRICANews",
        "zone":      "afrique",
        "categorie": "general",
        "langue":    "en",
        "priorite":  1,
    },
    {
        "name":      "Financial Times — World",
        "url":       "https://www.ft.com/rss/home/world",
        "zone":      "international",
        "categorie": "financier",
        "langue":    "en",
        "priorite":  1,
    },
    {
        "name":      "Financial Times — Markets",
        "url":       "https://www.ft.com/rss/home/markets",
        "zone":      "international",
        "categorie": "marche_financier",
        "langue":    "en",
        "priorite":  1,
    },
    {
        "name":      "Bloomberg — Markets",
        "url":       "https://feeds.bloomberg.com/markets/news.rss",
        "zone":      "international",
        "categorie": "marche_financier",
        "langue":    "en",
        "priorite":  1,
    },
    {
        "name":      "Bloomberg — World",
        "url":       "https://feeds.bloomberg.com/politics/news.rss",
        "zone":      "international",
        "categorie": "geopolitique",
        "langue":    "en",
        "priorite":  2,
    },
    {
        "name":      "CNBC — World Markets",
        "url":       "https://www.cnbc.com/id/100003114/device/rss/rss.html",
        "zone":      "international",
        "categorie": "marche_financier",
        "langue":    "en",
        "priorite":  1,
    },
    {
        "name":      "CNBC — Economy",
        "url":       "https://www.cnbc.com/id/20910258/device/rss/rss.html",
        "zone":      "international",
        "categorie": "économique",
        "langue":    "en",
        "priorite":  2,
    },
    {
        "name":      "MarketWatch — Top Stories",
        "url":       "https://feeds.marketwatch.com/marketwatch/topstories",
        "zone":      "international",
        "categorie": "marche_financier",
        "langue":    "en",
        "priorite":  1,
    },
    {
        "name":      "MarketWatch — Economy",
        "url":       "https://feeds.marketwatch.com/marketwatch/economy-politics",
        "zone":      "international",
        "categorie": "économique",
        "langue":    "en",
        "priorite":  2,
    },
    {
        "name":      "Yahoo Finance — News",
        "url":       "https://finance.yahoo.com/news/rssindex",
        "zone":      "international",
        "categorie": "marche_financier",
        "langue":    "en",
        "priorite":  2,
    },
    {
        "name":      "Investing.com — News",
        "url":       "https://www.investing.com/rss/news.rss",
        "zone":      "international",
        "categorie": "marche_financier",
        "langue":    "en",
        "priorite":  2,
    },
    {
        "name":      "Nasdaq — News",
        "url":       "https://www.nasdaq.com/feed/rssoutbound?category=Markets",
        "zone":      "international",
        "categorie": "marche_financier",
        "langue":    "en",
        "priorite":  2,
    },

    # ── INSTITUTIONS INTERNATIONALES ────────────────────────────────────────
    {
        "name":      "FMI — Communiqués",
        "url":       "https://www.imf.org/en/News/rss?selectedTypes=PressRelease",
        "zone":      "international",
        "categorie": "politique_monetaire",
        "langue":    "en",
        "priorite":  1,
    },
    {
        "name":      "Banque Mondiale — Afrique",
        "url":       "https://blogs.worldbank.org/africacan/rss.xml",
        "zone":      "afrique",
        "categorie": "économique",
        "langue":    "en",
        "priorite":  2,
    },
    {
        "name":      "BCE — Communiqués",
        "url":       "https://www.ecb.europa.eu/rss/press.html",
        "zone":      "international",
        "categorie": "politique_monetaire",
        "langue":    "en",
        "priorite":  1,
    },
    {
        "name":      "Fed — Communiqués (FRED)",
        "url":       "https://research.stlouisfed.org/rss/fred/",
        "zone":      "international",
        "categorie": "politique_monetaire",
        "langue":    "en",
        "priorite":  2,
    },

    # ── MATIÈRES PREMIÈRES (cacao, caoutchouc, pétrole, or) ──────────────
    {
        "name":      "Agence Ecofin — Cacao/Café",
        "url":       "https://www.agenceecofin.com/cacao/rss",
        "zone":      "uemoa",
        "categorie": "matieres_premieres",
        "langue":    "fr",
        "priorite":  1,
    },
    {
        "name":      "Agence Ecofin — Pétrole/Gaz",
        "url":       "https://www.agenceecofin.com/hydrocarbures/rss",
        "zone":      "international",
        "categorie": "matieres_premieres",
        "langue":    "fr",
        "priorite":  1,
    },
    {
        "name":      "Agence Ecofin — Or/Mines",
        "url":       "https://www.agenceecofin.com/or/rss",
        "zone":      "international",
        "categorie": "matieres_premieres",
        "langue":    "fr",
        "priorite":  2,
    },
    {
        "name":      "Commodities — Reuters",
        "url":       "https://feeds.reuters.com/reuters/commodities",
        "zone":      "international",
        "categorie": "matieres_premieres",
        "langue":    "en",
        "priorite":  1,
    },

    # ── GÉOPOLITIQUE & CONFLITS ─────────────────────────────────────────────
    {
        "name":      "Al Jazeera — Actualités",
        "url":       "https://www.aljazeera.com/xml/rss/all.xml",
        "zone":      "international",
        "categorie": "geopolitique",
        "langue":    "en",
        "priorite":  2,
    },
    {
        "name":      "BBC News — World",
        "url":       "http://feeds.bbci.co.uk/news/world/rss.xml",
        "zone":      "international",
        "categorie": "geopolitique",
        "langue":    "en",
        "priorite":  2,
    },
    {
        "name":      "BBC Afrique",
        "url":       "https://feeds.bbci.co.uk/afrique/rss.xml",
        "zone":      "afrique",
        "categorie": "general",
        "langue":    "fr",
        "priorite":  1,
    },
    {
        "name":      "Challenges — Économie monde",
        "url":       "https://www.challenges.fr/rss.xml",
        "zone":      "international",
        "categorie": "économique",
        "langue":    "fr",
        "priorite":  3,
    },
    {
        "name":      "Les Echos — Marchés",
        "url":       "https://www.lesechos.fr/rss/rss_marches.xml",
        "zone":      "international",
        "categorie": "marche_financier",
        "langue":    "fr",
        "priorite":  2,
    },
    {
        "name":      "Les Echos — Économie",
        "url":       "https://www.lesechos.fr/rss/rss_economie.xml",
        "zone":      "international",
        "categorie": "économique",
        "langue":    "fr",
        "priorite":  2,
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
    Collecte les actualités macro-économiques depuis des flux RSS,
    analyse leur impact via IA et insère dans google_alerts_rapports.
    """

    def __init__(self, db_conn, gemini_keys: list, deepseek_key: str,
                 mistral_key: str, max_articles_per_source: int = 8):
        self.db_conn   = db_conn
        self.gemini_keys   = gemini_keys if isinstance(gemini_keys, list) else [gemini_keys]
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
        logging.info("🌍 MACRO COLLECTOR — Démarrage collecte RSS")
        logging.info("="*60)

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
                    self.stats["errors"] += 1

        logging.info("="*60)
        logging.info(f"✅ Collecte terminée — Insérés: {self.stats['inserted']} | "
                     f"Ignorés: {self.stats['skipped']} | Erreurs: {self.stats['errors']}")
        logging.info("="*60)
        return self.stats

    # ──────────────────────────────────────────────────────────────────────────
    # CRÉATION / VÉRIFICATION DES COLONNES SUPABASE
    # ──────────────────────────────────────────────────────────────────────────

    def _ensure_table_columns(self):
        """
        Vérifie et crée les colonnes nécessaires dans google_alerts_rapports.
        Colonnes ajoutées si absentes :
          - zone         : TEXT  (international / afrique / uemoa)
          - sous_categorie : TEXT (geopolitique / matieres_premieres / etc.)
          - impact_brvm  : TEXT  (positif / negatif / neutre)
          - impact_bourses_mondiales : TEXT
          - impact_secteurs_brvm : JSONB  [{secteur, impact, justification}]
          - impact_societes_cotees : JSONB [{symbole, impact, raison}]
          - source_rss   : TEXT  (nom de la source RSS)
          - collecte_date : TIMESTAMPTZ
          - langue       : TEXT  (fr / en)
          - score_importance : INTEGER (0-100)
        """
        logging.info("🔧 Vérification colonnes google_alerts_rapports...")

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
            ("url_hash",                   "TEXT"),   # pour déduplication rapide
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
                logging.warning(f"⚠️  Colonne {col_name}: {e}")

        # Index sur url_hash pour déduplication rapide
        try:
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_gar_url_hash
                ON google_alerts_rapports(url_hash);
            """)
            self.db_conn.commit()
        except Exception:
            self.db_conn.rollback()

        cursor.close()
        logging.info("✅ Colonnes vérifiées / créées")

    # ──────────────────────────────────────────────────────────────────────────
    # COLLECTE RSS
    # ──────────────────────────────────────────────────────────────────────────

    def _fetch_all_rss(self) -> list:
        """Parcourt toutes les sources RSS et retourne une liste d'articles bruts."""
        all_articles = []
        cutoff = datetime.now(timezone.utc) - timedelta(days=3)

        for source in RSS_SOURCES:
            try:
                articles = self._fetch_one_rss(source, cutoff)
                all_articles.extend(articles)
                self.stats["fetched"] += len(articles)
                logging.info(
                    f"   📡 {source['name']:<40} → {len(articles)} article(s)"
                )
                time.sleep(0.3)
            except Exception as e:
                logging.warning(f"   ⚠️  {source['name']}: {e}")
                self.stats["errors"] += 1

        return all_articles

    def _fetch_one_rss(self, source: dict, cutoff: datetime) -> list:
        """Collecte les articles d'une source RSS unique."""
        headers = {
            'User-Agent': (
                'Mozilla/5.0 (compatible; BRVMBot/1.0; '
                '+https://brvm.org)'
            ),
            'Accept': 'application/rss+xml, application/xml, text/xml',
        }

        try:
            resp = requests.get(source["url"], headers=headers, timeout=15)
            resp.raise_for_status()
            feed = feedparser.parse(resp.text)
        except requests.exceptions.HTTPError as e:
            # Certains sites bloquent les bots → on continue sans erreur fatale
            logging.debug(f"HTTP {e.response.status_code} pour {source['name']}")
            return []
        except Exception as e:
            raise e

        articles = []
        for entry in feed.entries[:self.max_per_src]:
            # Date de publication
            pub_date = None
            if hasattr(entry, 'published_parsed') and entry.published_parsed:
                pub_date = datetime(*entry.published_parsed[:6], tzinfo=timezone.utc)
            elif hasattr(entry, 'updated_parsed') and entry.updated_parsed:
                pub_date = datetime(*entry.updated_parsed[:6], tzinfo=timezone.utc)

            # Filtrer les articles trop anciens
            if pub_date and pub_date < cutoff:
                continue

            # Contenu
            titre   = getattr(entry, 'title', '') or ''
            resume  = ''
            if hasattr(entry, 'summary'):
                resume = re.sub(r'<[^>]+>', '', entry.summary or '')[:1000]
            elif hasattr(entry, 'description'):
                resume = re.sub(r'<[^>]+>', '', entry.description or '')[:1000]

            url = getattr(entry, 'link', '') or ''

            if not titre and not resume:
                continue

            # Hash URL pour déduplication
            url_hash = hashlib.md5(url.encode()).hexdigest() if url else \
                       hashlib.md5(titre.encode()).hexdigest()

            # Score de pertinence préliminaire
            score = self._score_article(titre + ' ' + resume, source)

            articles.append({
                "mail_date":    pub_date or datetime.now(timezone.utc),
                "mail_subject": titre,
                "titre":        titre,
                "resume":       resume,
                "source_url":   url,
                "url_hash":     url_hash,
                "source_rss":   source["name"],
                "zone":         source["zone"],
                "sous_categorie": source["categorie"],
                "langue":       source["langue"],
                "score_importance": score,
                "alert_keyword":  source["categorie"],
                "mot_cle":       source["zone"],
                "categorie":    source["categorie"],
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
        - Impact sur les bourses mondiales
        - Impact par secteur BRVM [{secteur, impact, justification}]
        - Impact par société cotée [{symbole, impact, raison}]
        - Score de pertinence affiné
        - Points clés [{point1}, {point2}, ...]
        """
        titre  = article.get("titre", "")
        resume = article.get("resume", "")
        zone   = article.get("zone", "")
        cat    = article.get("sous_categorie", "")
        langue = article.get("langue", "fr")

        if not titre and not resume:
            return article

        # Si score trop faible → pas d'appel IA pour économiser les quotas
        if article.get("score_importance", 0) < 25:
            article["sentiment"] = "neutre"
            article["impact_brvm"] = "neutre"
            article["points_cles"] = []
            return article

        prompt = f"""Tu es un analyste financier expert de la BRVM (Bourse Régionale des Valeurs Mobilières d'Afrique de l'Ouest) et des marchés africains.

ARTICLE À ANALYSER :
Titre    : {titre}
Zone     : {zone}
Catégorie: {cat}
Langue   : {langue}
Contenu  : {resume[:800]}

SOCIÉTÉS COTÉES BRVM (référence) :
Banques : SGBC, BICB, BICC, BOAB, BOABF, BOAC, BOAM, BOAN, BNTS, CBIB, ECOC, NSBC, OREC, SAFC
Industrie : ABJC, CABC, NTLC, ORAC, SAPH, SICC, SPHC, SIVC, TTLS, UNLC, STAC, SCRC
Énergie/Distrib : ETIT, SDSC, SHEC, TOTAL
Agro : PALC, SOGC, TRDE, LNBB
Télécom : ONTBF, SNTS
Autres : CFAC, PRSC, SVOC

MISSION — Réponds UNIQUEMENT en JSON valide, sans explication, sans markdown :
{{
  "resume_fr": "résumé en français 3-5 phrases, factuel et précis",
  "points_cles": ["point 1", "point 2", "point 3"],
  "sentiment": "positif|negatif|neutre",
  "pertinence": <entier 0-100>,
  "impact_brvm": "positif|negatif|neutre",
  "impact_brvm_detail": "explication 2-3 phrases de l'impact sur la BRVM",
  "impact_bourses_mondiales": "explication 1-2 phrases de l'impact sur NYSE/Euronext/marchés asiatiques",
  "impact_secteurs_brvm": [
    {{"secteur": "Banques", "impact": "positif|negatif|neutre", "justification": "phrase courte"}},
    {{"secteur": "Industrie", "impact": "positif|negatif|neutre", "justification": "phrase courte"}},
    {{"secteur": "Matières premières", "impact": "positif|negatif|neutre", "justification": "phrase courte"}}
  ],
  "impact_societes_cotees": [
    {{"symbole": "SGBC", "impact": "positif|negatif|neutre", "raison": "phrase courte"}},
    {{"symbole": "SPHC", "impact": "positif|negatif|neutre", "raison": "phrase courte"}}
  ],
  "score_importance": <entier 0-100>
}}
Ne cite que les sociétés réellement concernées (max 5). Si l'article n'a aucun lien avec la BRVM, laisse impact_societes_cotees vide."""

        # Tentative avec rotation des IA
        result_json = None
        for fn in [self._call_gemini, self._call_deepseek, self._call_mistral]:
            try:
                raw = fn(prompt)
                if raw:
                    # Nettoyer le JSON
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
            # Fallback sans IA : sentiment par mots-clés
            text = (titre + " " + resume).lower()
            pos_kw = ["hausse", "croissance", "positif", "bénéfice", "accord",
                      "investissement", "record", "stabilité", "growth", "rise"]
            neg_kw = ["baisse", "chute", "crise", "conflit", "guerre", "sanction",
                      "récession", "déficit", "perte", "fall", "drop", "war"]
            pos_hits = sum(1 for k in pos_kw if k in text)
            neg_hits = sum(1 for k in neg_kw if k in text)
            article["sentiment"] = "positif" if pos_hits > neg_hits else \
                                   "negatif" if neg_hits > pos_hits else "neutre"
            article["impact_brvm"]       = article["sentiment"]
            article["points_cles"]       = []
            article["impact_secteurs_brvm"]   = []
            article["impact_societes_cotees"] = []

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
                ON CONFLICT DO NOTHING;
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
# SQL DE MIGRATION — À exécuter UNE SEULE FOIS dans Supabase
# ==============================================================================
SQL_MIGRATION = """
-- ============================================================
-- MIGRATION google_alerts_rapports
-- Ajouter les colonnes nécessaires au macro_collector
-- À exécuter dans l'éditeur SQL de Supabase
-- ============================================================

ALTER TABLE google_alerts_rapports
    ADD COLUMN IF NOT EXISTS zone                     TEXT,
    ADD COLUMN IF NOT EXISTS sous_categorie           TEXT,
    ADD COLUMN IF NOT EXISTS impact_brvm              TEXT DEFAULT 'neutre',
    ADD COLUMN IF NOT EXISTS impact_bourses_mondiales TEXT,
    ADD COLUMN IF NOT EXISTS impact_secteurs_brvm     JSONB,
    ADD COLUMN IF NOT EXISTS impact_societes_cotees   JSONB,
    ADD COLUMN IF NOT EXISTS source_rss               TEXT,
    ADD COLUMN IF NOT EXISTS collecte_date            TIMESTAMPTZ DEFAULT NOW(),
    ADD COLUMN IF NOT EXISTS langue                   TEXT DEFAULT 'fr',
    ADD COLUMN IF NOT EXISTS score_importance         INTEGER DEFAULT 50,
    ADD COLUMN IF NOT EXISTS url_hash                 TEXT;

-- Index pour déduplication rapide
CREATE INDEX IF NOT EXISTS idx_gar_url_hash
    ON google_alerts_rapports(url_hash);

CREATE INDEX IF NOT EXISTS idx_gar_zone
    ON google_alerts_rapports(zone);

CREATE INDEX IF NOT EXISTS idx_gar_impact_brvm
    ON google_alerts_rapports(impact_brvm);

CREATE INDEX IF NOT EXISTS idx_gar_score
    ON google_alerts_rapports(score_importance DESC);

CREATE INDEX IF NOT EXISTS idx_gar_collecte_date
    ON google_alerts_rapports(collecte_date DESC);

-- Vue pratique pour le rapport
CREATE OR REPLACE VIEW v_macro_actualites AS
SELECT
    id,
    mail_date,
    zone,
    sous_categorie,
    titre,
    resume,
    sentiment,
    impact_brvm,
    impact_bourses_mondiales,
    impact_secteurs_brvm,
    impact_societes_cotees,
    points_cles,
    pertinence,
    score_importance,
    source_rss,
    source_url,
    langue,
    collecte_date
FROM google_alerts_rapports
WHERE collecte_date >= NOW() - INTERVAL '7 days'
ORDER BY score_importance DESC, mail_date DESC;
"""


# ==============================================================================
# SCRIPT STANDALONE — pour test ou appel direct
# ==============================================================================
if __name__ == "__main__":
    import os

    DB_URL          = os.environ.get("DATABASE_URL")
    GEMINI_KEYS     = [k for k in [
        os.environ.get("GEMINI_API_KEY"),
        os.environ.get("GEMINI_API_KEY_2"),
        os.environ.get("GEMINI_API_KEY_3"),
    ] if k]
    DEEPSEEK_KEY    = os.environ.get("DEEPSEEK_API_KEY")
    MISTRAL_KEY     = os.environ.get("MISTRAL_API_KEY")

    if not DB_URL:
        print("❌ DATABASE_URL non défini")
        exit(1)

    conn = psycopg2.connect(DB_URL)
    collector = MacroCollector(
        db_conn=conn,
        gemini_keys=GEMINI_KEYS,
        deepseek_key=DEEPSEEK_KEY,
        mistral_key=MISTRAL_KEY,
        max_articles_per_source=8,
    )

    stats = collector.run()
    print(f"\n📊 Résultats : {stats}")

    # Afficher la migration SQL
    print("\n" + "="*60)
    print("SQL DE MIGRATION (déjà exécuté automatiquement par le code)")
    print("="*60)
    print(SQL_MIGRATION)
    conn.close()
