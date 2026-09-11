# Réestimation mensuelle des modèles BRVM

Pipeline GitHub Actions interne à `brvm-analysis-suite`. Chaque 1er du mois il :

1. évalue la qualité des prédictions du mois civil précédent (`predictions` vs `historical_data`) ;
2. réestime en **warm-start** (fine-tuning sur les 100 dernières cotations) **uniquement** les modèles jugés défaillants ;
3. historise chaque évaluation dans `model_evaluations` ;
4. envoie un mail de récapitulatif détaillé ;
5. committe dans `modeles/` **seulement** les `model_GRU.keras` réellement modifiés.

## Arborescence à committer

```
brvm-analysis-suite/
├── .github/workflows/monthly-retrain.yml
└── retraining/
    ├── run.py            # orchestrateur
    ├── evaluate.py       # métriques + verdict
    ├── retrain.py        # warm-start fine-tuning
    ├── notify.py         # mail HTML (Gmail/SMTP)
    ├── db.py             # accès Supabase
    ├── requirements.txt
    ├── sql/model_evaluations.sql
    └── README.md
```

## Mise en route

1. **Créer la table d'historique** : exécuter `sql/model_evaluations.sql` dans Supabase.
2. **Secrets du dépôt** (Settings → Secrets and variables → Actions) :
   `SUPABASE_URL`, `SUPABASE_KEY`, `GMAIL_USER`, `GMAIL_APP_PASSWORD` (mot de passe
   d'application Gmail, comme tes Agents 2/3), `MAIL_TO` (destinataire(s), séparés par des virgules).
3. **Lancer un test** via l'onglet Actions → *Monthly model retrain & eval* → *Run workflow*.

## Règle de verdict

Un modèle est **OK** si les trois conditions sont réunies, sinon **défaillant** :

| Métrique              | Seuil OK   |
|-----------------------|------------|
| MAPE                  | < 10 %     |
| MAE relative          | < 10 %     |
| Accuracy directionnelle | > 70 %   |

En dessous de 5 points de comparaison sur le mois, le verdict est **insuffisant** et
le modèle n'est pas réestimé (signalé dans le mail). Seuils ajustables dans
`evaluate.py` (`THRESHOLDS`).

## À vérifier de ton côté

1. **Features / cible** (`retrain.py`, constante `FEATURES`). Par défaut :
   `["price", "volume", "value", "company_capitalization"]`, `price` en tête = cible.
   - Si tes modèles sont **univariés** (entrée = prix seul), rien à faire : le code lit
     `n_features == 1` dans le `.keras` et n'utilise que `price`.
   - S'ils sont **multivariés**, vérifie que l'ordre ci-dessus correspond exactement à
     celui utilisé lors de l'entraînement d'origine. En cas d'écart entre le nombre de
     features du scaler et du modèle, le titre est ignoré proprement (reporté dans le mail),
     pas de modèle corrompu.
2. **Version de TensorFlow/Keras**. `requirements.txt` fixe `tensorflow-cpu==2.16.1`
   (Keras 3, format `.keras` natif). Si tes modèles ont été entraînés avec une autre
   version, aligne cette ligne sur la version d'origine pour éviter les erreurs de chargement.
3. **Scaler réutilisé, pas refit.** Le warm-start réutilise le `scaler.pkl` existant
   (cohérence avec les poids). Si tu préfères refit le scaler sur les 100 jours et le
   réenregistrer, on ajoute quelques lignes dans `retrain.py` (`scaler.fit` + `pickle.dump`).

## Hyperparamètres du fine-tuning (`retrain.py`)

`FINE_TUNE_LR = 1e-4`, `EPOCHS = 25`, `BATCH = 16`, `EarlyStopping(patience=5)`.
LR volontairement faible pour rafraîchir sans effacer la mémoire longue.
