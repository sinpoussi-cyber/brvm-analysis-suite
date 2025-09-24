# Suite d'Analyse Complète et Automatisée pour la BRVM

Ce projet est une suite logicielle entièrement automatisée qui collecte, analyse et synthétise des données sur les sociétés cotées à la Bourse Régionale des Valeurs Mobilières (BRVM). Le processus complet s'exécute quotidiennement via GitHub Actions et produit trois rapports d'investissement distincts qui sont sauvegardés sur Google Drive.

## 🏗️ Architecture et Fonctionnalités

La suite est orchestrée par le script `main.py` et se déroule en quatre étapes séquentielles :

1.  **Collecte de Données (`data_collector.py`)** : Scrape les données de marché quotidiennes et les archive dans un Google Sheet.
2.  **Analyse Technique (`technical_analyzer.py`)** : Calcule les indicateurs techniques (Moyennes Mobiles, Bollinger, MACD, RSI, Stochastique) et les sauvegarde dans le Google Sheet.
3.  **Analyse Fondamentale (`fundamental_analyzer.py`)** : Scrape les rapports financiers des sociétés et utilise l'IA (Google Gemini) pour les synthétiser.
4.  **Génération des Rapports (`report_generator.py`)** : Utilise toutes les données collectées pour générer trois documents Word :
    *   **Rapport de Synthèse Complet** : Une analyse détaillée pour chaque société (cours, technique, fondamental) et une synthèse globale du marché.
    *   **Rapport Comparatif (Delta)** : Une analyse des changements significatifs par rapport au rapport de la veille.
    *   **Synthèse des Événements Marquants** : Un résumé des nouvelles analyses fondamentales du jour.

### Fonctionnalités Avancées
- **Automatisation Complète** : Le workflow s'exécute chaque jour sans aucune intervention manuelle.
- **Sauvegarde sur Google Drive** : Tous les rapports générés sont automatiquement sauvegardés dans un Drive Partagé.
- **Mémoire Persistante** : Le système mémorise les rapports déjà analysés dans une feuille Google Sheet (`ANALYSIS_MEMORY`) pour ne pas les ré-analyser.
- **Rotation de Clés API** : Gère une liste de plusieurs clés API Gemini pour contourner les limites de quota journalières et par minute.

## ⚙️ Configuration Initiale

Suivez ces étapes pour rendre le projet opérationnel.

### Étape 1 : Prérequis

-   Un compte GitHub.
-   Un ou plusieurs projets sur [Google Cloud Platform](https://console.cloud.google.com/).
-   Un compte Google Workspace ou un compte Google personnel pouvant créer des Drives Partagés.

### Étape 2 : Configuration du Compte de Service Google

Ce compte est le "bot" qui agira en votre nom.

1.  **Créez un Compte de Service** dans un de vos projets Google Cloud et donnez-lui le rôle **"Éditeur"**.
2.  **Générez une Clé JSON** pour ce compte et téléchargez-la.
3.  **Partagez votre Google Sheet** :
    -   Créez un nouveau Google Sheet pour stocker vos données.
    -   Ouvrez le fichier JSON et copiez l'adresse e-mail de la ligne `"client_email"`.
    -   Dans votre Google Sheet, cliquez sur **"Partager"**, collez l'adresse e-mail, donnez-lui les droits **"Éditeur"**, et envoyez.

### Étape 3 : Configuration du Drive Partagé

1.  Allez sur [Google Drive](https://drive.google.com/).
2.  Dans le menu de gauche, cliquez sur **"Drives partagés"**.
3.  Créez un nouveau Drive Partagé (ex: `Rapports BRVM`).
4.  Ouvrez ce Drive Partagé, cliquez sur son nom en haut, puis sur **"Gérer les membres"**.
5.  **Ajoutez l'adresse e-mail de votre compte de service** et donnez-lui le rôle **"Gestionnaire de contenu"**.
6.  Créez un dossier à l'intérieur de ce Drive Partagé (ex: `Rapports Journaliers`).
7.  Ouvrez ce dossier et **copiez l'identifiant de ce dossier** depuis l'URL (la chaîne de caractères à la fin).

### Étape 4 : Création des Clés API Gemini

Pour des quotas séparés, il est recommandé de créer chaque clé dans un projet Google Cloud différent.
1.  Dans chaque projet, activez l'**API "Vertex AI"**.
2.  Créez une **Clé d'API** depuis la section "Identifiants".

### Étape 5 : Configuration des Secrets GitHub

Dans votre dépôt GitHub, allez dans `Settings` -> `Secrets and variables` -> `Actions` et créez les secrets suivants :

-   `GSPREAD_SERVICE_ACCOUNT`: Le contenu complet de votre fichier `.json` de compte de service.
-   `SPREADSHEET_ID`: L'identifiant de votre Google Sheet.
-   `DRIVE_FOLDER_ID`: L'identifiant de votre dossier dans le Drive Partagé.
-   `GOOGLE_API_KEY_1`, `GOOGLE_API_KEY_2`, etc. : Vos différentes clés API Gemini.

## 🚀 Exécution

Le workflow s'exécute automatiquement chaque jour. Les trois rapports générés sont disponibles dans les **Artifacts** de chaque exécution et sont sauvegardés dans votre Google Drive.

## 📁 Structure du Projet

```brvm-analysis-suite/
│
├── .github/workflows/daily_brvm_analysis.yml
├── main.py
├── data_collector.py
├── technical_analyzer.py
├── fundamental_analyzer.py
├── report_generator.py
├── requirements.txt
└── README.md
