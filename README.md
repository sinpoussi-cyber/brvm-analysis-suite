# 📊 BRVM Analysis Suite

Suite logicielle automatisée pour la collecte, l'analyse et la synthèse des données des sociétés cotées à la Bourse Régionale des Valeurs Mobilières (BRVM).

## 🎯 Objectif

Ce projet constitue le **moteur backend** d'une plateforme FinTech complète pour le marché de l'UEMOA. Il exécute quotidiennement une analyse complète de toutes les sociétés cotées à la BRVM et alimente une base de données centrale PostgreSQL.

## 🏗️ Architecture

### Architecture Technique
```
┌─────────────────────────────────────────────────────────────┐
│                    GitHub Actions                            │
│              (Exécution quotidienne à 20h UTC)              │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                         main.py                              │
│                   (Orchestrateur principal)                  │
└─────────────────────────────────────────────────────────────┘
                              │
        ┌─────────────────────┼─────────────────────┐
        ▼                     ▼                     ▼
┌──────────────┐    ┌──────────────┐    ┌──────────────┐
│ data_        │    │ technical_   │    │ fundamental_ │
│ collector.py │───▶│ analyzer.py  │───▶│ analyzer.py  │
└──────────────┘    └──────────────┘    └──────────────┘
        │                   │                     │
        └───────────────────┼─────────────────────┘
                            ▼
              ┌──────────────────────────┐
              │  PostgreSQL (Supabase)   │
              │  - historical_data       │
              │  - technical_analysis    │
              │  - fundamental_analysis  │
              └──────────────────────────┘
                            │
        ┌───────────────────┼───────────────────┐
        ▼                   ▼                   ▼
┌─────────────┐   ┌─────────────┐   ┌─────────────┐
│  report_    │   │  export_to_ │   │  API        │
│  generator  │   │  gsheet.py  │   │  Gateway    │
└─────────────┘   └─────────────┘   └─────────────┘
```

## ⚙️ Modules et Fonctionnalités

### 1. **data_collector.py** - Collecte des Données
- 📥 Scrape les bulletins officiels de la cote (BOC) de la BRVM
- 📄 Extrait les données de marché quotidiennes (cours, volume, valeur)
- 💾 Stocke les données dans PostgreSQL avec déduplication automatique
- 🔄 Traite les 15 bulletins les plus récents

### 2. **technical_analyzer.py** - Analyse Technique
- 📊 Calcule 5 indicateurs techniques majeurs :
  - Moyennes Mobiles (MM5, MM10, MM20, MM50)
  - Bandes de Bollinger
  - MACD (Moving Average Convergence Divergence)
  - RSI (Relative Strength Index)
  - Oscillateur Stochastique
- 🎯 Génère des signaux d'achat/vente/neutre pour chaque indicateur
- 💾 Stocke les analyses dans la table `technical_analysis`

### 3. **fundamental_analyzer.py** - Analyse Fondamentale (IA)
- 🤖 Utilise Google Gemini pour analyser les rapports financiers
- 📑 Scrape automatiquement les rapports depuis le site de la BRVM
- 🧠 Extrait les informations clés :
  - Évolution du chiffre d'affaires
  - Résultat net
  - Politique de dividende
  - Perspectives
- 🔄 Rotation automatique entre plusieurs clés API (gestion des quotas)
- 💾 Mémorise les rapports déjà analysés pour éviter les doublons

### 4. **report_generator.py** - Génération de Rapports
- 📝 Génère un rapport Word complet pour chaque société
- 🎨 Mise en page professionnelle avec sections structurées
- 📊 Inclut :
  - Analyse de l'évolution des prix (50 jours)
  - Synthèse des indicateurs techniques
  - Résumé des analyses fondamentales
- 📤 Rapports disponibles dans les artifacts GitHub Actions

### 5. **export_to_gsheet.py** - Export Google Sheets
- 📤 Exporte une copie des données quotidiennes vers Google Sheets
- 📋 Une feuille par société pour faciliter la consultation
- 🔄 Backup supplémentaire des données

## 🚀 Workflow d'Exécution

Le workflow s'exécute automatiquement **du lundi au vendredi à 20h00 UTC** :

```
1. Collecte des données (data_collector.py)
   └─ Télécharge et parse les BOCs
   └─ Insère dans historical_data

2. Analyse technique (technical_analyzer.py)
   └─ Calcule les indicateurs
   └─ Insère dans technical_analysis

3. Analyse fondamentale (fundamental_analyzer.py)
   └─ Scrape les rapports
   └─ Analyse via IA Gemini
   └─ Insère dans fundamental_analysis

4. Génération de rapports (report_generator.py)
   └─ Génère les synthèses IA
   └─ Crée le rapport Word

5. Export Google Sheets (export_to_gsheet.py)
   └─ Exporte les données du jour
```

## 📦 Installation et Configuration

### Prérequis

- Un compte GitHub
- Un projet Google Cloud Platform (pour les clés API Gemini)
- Une base de données PostgreSQL (Supabase recommandé)
- (Optionnel) Un Google Sheet pour l'export

### Étape 1 : Fork ou Clone du Dépôt

```bash
git clone https://github.com/votre-username/brvm-analysis-suite.git
cd brvm-analysis-suite
```

### Étape 2 : Configuration de la Base de Données

1. **Créez un projet sur [Supabase](https://supabase.com)**
2. **Créez les tables nécessaires** (voir section Schéma de Base de Données)
3. **Notez vos identifiants de connexion**

### Étape 3 : Configuration des Secrets GitHub

Allez dans `Settings` → `Secrets and variables` → `Actions` et créez :

#### Secrets de Base de Données (OBLIGATOIRES)
- `DB_NAME` : Nom de votre base (généralement `postgres`)
- `DB_USER` : Utilisateur (généralement `postgres`)
- `DB_PASSWORD` : Votre mot de passe Supabase
- `DB_HOST` : Hôte (ex: `db.xxxxx.supabase.co`)
- `DB_PORT` : Port (généralement `5432`)

#### Clés API Gemini (OBLIGATOIRES pour analyses IA)
- `GOOGLE_API_KEY_1` : Votre première clé API
- `GOOGLE_API_KEY_2` : Deuxième clé (optionnel)
- `GOOGLE_API_KEY_3` : Troisième clé (optionnel)
- etc.

**Comment obtenir une clé API Gemini :**
1. Allez sur [Google AI Studio](https://aistudio.google.com/app/apikey)
2. Créez une nouvelle clé API
3. Copiez la clé

#### Secrets Google Sheets (OPTIONNELS)
- `GSPREAD_SERVICE_ACCOUNT` : Contenu du fichier JSON du compte de service
- `SPREADSHEET_ID` : ID de votre Google Sheet

### Étape 4 : Activation du Workflow

Le workflow est déjà configuré dans `.github/workflows/daily_brvm_analysis.yml`.

Pour un test manuel :
1. Allez dans l'onglet **Actions**
2. Sélectionnez **Analyse Quotidienne BRVM**
3. Cliquez sur **Run workflow**

## 🗄️ Schéma de Base de Données

### Table `companies`
```sql
CREATE TABLE companies (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(10) UNIQUE NOT NULL,
    name VARCHAR(255) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### Table `historical_data`
```sql
CREATE TABLE historical_data (
    id SERIAL PRIMARY KEY,
    company_id INTEGER REFERENCES companies(id),
    trade_date DATE NOT NULL,
    price DECIMAL(10, 2),
    volume INTEGER,
    value DECIMAL(15, 2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(company_id, trade_date)
);

CREATE INDEX idx_historical_data_date ON historical_data(trade_date DESC);
CREATE INDEX idx_historical_data_company ON historical_data(company_id);
```

### Table `technical_analysis`
```sql
CREATE TABLE technical_analysis (
    id SERIAL PRIMARY KEY,
    historical_data_id INTEGER UNIQUE REFERENCES historical_data(id),
    mm5 DECIMAL(10, 2),
    mm10 DECIMAL(10, 2),
    mm20 DECIMAL(10, 2),
    mm50 DECIMAL(10, 2),
    mm_decision VARCHAR(50),
    bollinger_central DECIMAL(10, 2),
    bollinger_inferior DECIMAL(10, 2),
    bollinger_superior DECIMAL(10, 2),
    bollinger_decision VARCHAR(50),
    macd_line DECIMAL(10, 4),
    signal_line DECIMAL(10, 4),
    histogram DECIMAL(10, 4),
    macd_decision VARCHAR(50),
    rsi DECIMAL(5, 2),
    rsi_decision VARCHAR(50),
    stochastic_k DECIMAL(5, 2),
    stochastic_d DECIMAL(5, 2),
    stochastic_decision VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### Table `fundamental_analysis`
```sql
CREATE TABLE fundamental_analysis (
    id SERIAL PRIMARY KEY,
    company_id INTEGER REFERENCES companies(id),
    report_url VARCHAR(500) UNIQUE NOT NULL,
    report_title VARCHAR(500),
    report_date DATE,
    analysis_summary TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_fundamental_company ON fundamental_analysis(company_id);
```

## 📊 Données Disponibles

Le système collecte et analyse les données de **toutes les sociétés cotées à la BRVM**, incluant mais non limité à :

- 🏦 **Secteur Bancaire** : SGBC, BOAC, BICC, NSBC, ECOC, etc.
- 📡 **Télécommunications** : SNTS, ORAC, ONTBF
- 🏭 **Industrie** : PALC, NTLC, UNLC, SLBC, SICC
- ⚡ **Énergie** : TTLC, TTLS, SHEC, CIEC
- 🏢 **Distribution** : CFAC, PRSC, SDSC

## 🔐 Sécurité et Bonnes Pratiques

- ✅ Tous les secrets sont stockés dans GitHub Secrets (jamais dans le code)
- ✅ Connexions sécurisées à la base de données
- ✅ Gestion automatique des quotas API
- ✅ Déduplication des données pour éviter les doublons
- ✅ Logs détaillés pour le debugging

## 📈 Métriques et Monitoring

Chaque exécution génère des statistiques :
- Nombre de bulletins traités
- Nombre de nouvelles données collectées
- Nombre d'analyses techniques calculées
- Nombre de nouvelles analyses fondamentales
- Durée totale d'exécution

## 🐛 Dépannage

### Le workflow ne démarre pas
- Vérifiez que tous les secrets obligatoires sont configurés
- Vérifiez les permissions GitHub Actions dans Settings → Actions

### Erreurs de connexion DB
- Vérifiez vos identifiants Supabase
- Assurez-vous que votre IP n'est pas bloquée (Supabase accepte toutes les IPs par défaut)

### Quotas API Gemini atteints
- Ajoutez plus de clés API (`GOOGLE_API_KEY_4`, `GOOGLE_API_KEY_5`, etc.)
- Les clés sont utilisées en rotation automatique

### Aucune donnée collectée
- Vérifiez que le site de la BRVM est accessible
- Consultez les logs dans l'onglet Actions

## 🤝 Contribution

Les contributions sont les bienvenues ! N'hésitez pas à :
- Signaler des bugs
- Proposer de nouvelles fonctionnalités
- Améliorer la documentation

## 📄 Licence

Ce projet est sous licence MIT. Voir le fichier LICENSE pour plus de détails.

## 🔗 Liens Utiles

- [Site de la BRVM](https://www.brvm.org)
- [Documentation Supabase](https://supabase.com/docs)
- [Google AI Studio](https://aistudio.google.com)
- [GitHub Actions Documentation](https://docs.github.com/en/actions)

## 📧 Contact

Pour toute question ou suggestion, n'hésitez pas à ouvrir une issue sur GitHub.

---

**Fait avec ❤️ pour démocratiser l'accès à l'analyse financière en Afrique de l'Ouest**

Update
