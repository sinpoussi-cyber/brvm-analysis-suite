# Suite d'Analyse Complète pour la BRVM

Ce projet combine trois modules pour fournir une analyse complète et automatisée du marché de la Bourse Régionale des Valeurs Mobilières (BRVM). Le processus s'exécute quotidiennement via GitHub Actions.

## ⚙️ Fonctionnalités

La suite exécute les tâches suivantes dans l'ordre :

1.  **Collecte de Données (`data_collector.py`)**: Scrape les derniers Bulletins Officiels de la Cote (BOC) depuis le site de la BRVM, extrait les données de transactions (cours, volume, etc.) et les archive dans un Google Sheet.
2.  **Analyse Fondamentale (`fundamental_analyzer.py`)**: Scrape les rapports financiers des sociétés cotées, utilise l'IA (Google Gemini) pour en générer une synthèse (Chiffre d'affaires, Résultat net, Dividendes, Perspectives), et compile les résultats dans un rapport Microsoft Word.
3.  **Analyse Technique (`technical_analyzer.py`)**: Utilise les données historiques du Google Sheet pour calculer plusieurs indicateurs techniques (Moyennes Mobiles, Bandes de Bollinger, MACD, RSI, Stochastique) et les inscrit directement dans le Google Sheet.

## 🚀 Configuration Initiale

Suivez ces étapes pour rendre le projet opérationnel.

### Étape 1 : Prérequis

- Un compte Google.
- Un projet sur [Google Cloud Platform](https://console.cloud.google.com/).
- Un compte GitHub.

### Étape 2 : Configurer le Compte de Service Google

Ce script utilise un compte de service pour accéder à votre Google Sheet de manière sécurisée.

1.  **Créez un Compte de Service** :
    - Allez sur la [page des comptes de service](https://console.cloud.google.com/iam-admin/serviceaccounts) de Google Cloud.
    - Sélectionnez votre projet.
    - Cliquez sur **"+ CRÉER UN COMPTE DE SERVICE"**.
    - Donnez-lui un nom (ex: `brvm-suite-bot`) et cliquez sur **"CRÉER ET CONTINUER"**.
    - Pour le rôle, choisissez **"Éditeur" (Editor)**. Cliquez sur **"OK"**.

2.  **Générez une Clé JSON** :
    - Cliquez sur votre nouveau compte de service, allez dans l'onglet **"CLÉS"**.
    - Cliquez sur **"AJOUTER UNE CLÉ"** -> **"Créer une nouvelle clé"**.
    - Choisissez le format **JSON** et cliquez sur **"CRÉER"**. Un fichier `.json` sera téléchargé. Gardez-le précieusement.

3.  **Partagez votre Google Sheet** :
    - Créez un nouveau Google Sheet.
    - Ouvrez le fichier JSON téléchargé et copiez l'adresse e-mail de la ligne `"client_email"`.
    - Dans votre Google Sheet, cliquez sur **"Partager"**, collez l'adresse e-mail, donnez-lui les droits **"Éditeur"**, et envoyez.

### Étape 3 : Activer les APIs Google

Assurez-vous que les APIs suivantes sont activées pour votre projet Google Cloud :
- **Google Sheets API**
- **Google Drive API**
- **Vertex AI API** (pour l'utilisation de Gemini)

### Étape 4 : Ajouter les Secrets à GitHub

Dans votre dépôt GitHub, allez dans `Settings` -> `Secrets and variables` -> `Actions`. Vous devez créer **deux** secrets :

1.  **`GSPREAD_SERVICE_ACCOUNT`**:
    - **Nom**: `GSPREAD_SERVICE_ACCOUNT`
    - **Valeur**: Ouvrez le fichier `.json` que vous avez téléchargé, copiez **tout son contenu** et collez-le ici.

2.  **`GOOGLE_API_KEY`**:
    - **Nom**: `GOOGLE_API_KEY`
    - **Valeur**: [Créez une clé API](https://console.cloud.google.com/apis/credentials) dans votre projet Google Cloud et collez-la ici. Cette clé est nécessaire pour l'analyse par IA.

### Étape 5 : Activer et Tester le Workflow

1.  Allez dans l'onglet **Actions** de votre dépôt GitHub.
2.  Sur la gauche, cliquez sur **"Full BRVM Analysis Suite"**.
3.  Cliquez sur le bouton **Run workflow** pour lancer manuellement le script une première fois et vérifier que tout fonctionne.
4.  L'exécution peut être suivie en temps réel. Le rapport Word généré sera disponible dans les "Artifacts" à la fin de l'exécution.

Le script s'exécutera désormais automatiquement tous les jours à 07h00 UTC.
