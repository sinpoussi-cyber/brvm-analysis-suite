# ==============================================================================
# SCRIPT DE TEST - VÉRIFICATION DES 33 CLÉS API GEMINI (VERSION 7.3)
# ==============================================================================

import os
import requests
import logging
import time

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s')

def test_gemini_api_key(api_key, key_number):
    """
    Teste une clé API Gemini avec une requête simple
    Basé sur les versions disponibles: v1, v2, v2beta, v2internal, v3, v3beta
    """
    logging.info(f"\n{'='*60}")
    logging.info(f"TEST DE LA CLÉ API #{key_number}")
    logging.info(f"{'='*60}")
    
    # Masquer la clé pour la sécurité
    masked_key = api_key[:8] + "..." + api_key[-4:] if len(api_key) > 12 else "***"
    logging.info(f"Clé : {masked_key}")
    
    # URLs à tester (dans l'ordre de priorité basé sur VOS versions)
    test_urls = [
        # ✅ Option 1 : v2beta avec gemini-1.5-flash (RECOMMANDÉ)
        {
            "url": f"https://generativelanguage.googleapis.com/v2beta/models/gemini-1.5-flash:generateContent?key={api_key}",
            "model": "gemini-1.5-flash",
            "version": "v2beta"
        },
        # Option 2 : v2beta avec gemini-1.5-flash-latest
        {
            "url": f"https://generativelanguage.googleapis.com/v2beta/models/gemini-1.5-flash-latest:generateContent?key={api_key}",
            "model": "gemini-1.5-flash-latest",
            "version": "v2beta"
        },
        # Option 3 : v3beta avec gemini-1.5-flash
        {
            "url": f"https://generativelanguage.googleapis.com/v3beta/models/gemini-1.5-flash:generateContent?key={api_key}",
            "model": "gemini-1.5-flash",
            "version": "v3beta"
        },
        # Option 4 : v2 avec gemini-1.5-flash
        {
            "url": f"https://generativelanguage.googleapis.com/v2/models/gemini-1.5-flash:generateContent?key={api_key}",
            "model": "gemini-1.5-flash",
            "version": "v2"
        },
        # Option 5 : v1 avec gemini-1.5-flash
        {
            "url": f"https://generativelanguage.googleapis.com/v1/models/gemini-1.5-flash:generateContent?key={api_key}",
            "model": "gemini-1.5-flash",
            "version": "v1"
        },
        # Option 6 : v1 avec gemini-pro (modèle plus ancien mais stable)
        {
            "url": f"https://generativelanguage.googleapis.com/v1/models/gemini-pro:generateContent?key={api_key}",
            "model": "gemini-pro",
            "version": "v1"
        },
        # Option 7 : v3 avec gemini-1.5-flash
        {
            "url": f"https://generativelanguage.googleapis.com/v3/models/gemini-1.5-flash:generateContent?key={api_key}",
            "model": "gemini-1.5-flash",
            "version": "v3"
        },
    ]
    
    # Test simple
    test_request = {
        "contents": [{
            "parts": [{"text": "Dis bonjour en français"}]
        }],
        "generationConfig": {
            "temperature": 0.7,
            "maxOutputTokens": 50
        }
    }
    
    headers = {"Content-Type": "application/json"}
    
    for idx, test_config in enumerate(test_urls, 1):
        url = test_config["url"]
        model_name = test_config["model"]
        api_version = test_config["version"]
        
        logging.info(f"\n  Test {idx}/{len(test_urls)} : Modèle '{model_name}' (API {api_version})")
        
        try:
            response = requests.post(url, json=test_request, headers=headers, timeout=15)
            
            if response.status_code == 200:
                logging.info(f"    ✅ SUCCÈS ! Ce modèle fonctionne")
                try:
                    result = response.json()
                    answer = result['candidates'][0]['content']['parts'][0]['text']
                    logging.info(f"    📝 Réponse: {answer[:50]}...")
                    return True, test_config  # Retourner la config qui fonctionne
                except:
                    logging.info(f"    ⚠️  Réponse reçue mais format inattendu")
                    
            elif response.status_code == 404:
                logging.warning(f"    ❌ 404 - Modèle non trouvé ou API version non activée")
                
            elif response.status_code == 403:
                logging.error(f"    ❌ 403 - Accès refusé. Vérifiez les permissions de la clé")
                
            elif response.status_code == 429:
                logging.warning(f"    ⚠️  429 - Quota dépassé")
                
            else:
                logging.warning(f"    ❌ Erreur {response.status_code}: {response.text[:100]}")
                
        except requests.exceptions.Timeout:
            logging.error(f"    ❌ Timeout - Le serveur ne répond pas")
        except requests.exceptions.ConnectionError:
            logging.error(f"    ❌ Erreur de connexion")
        except Exception as e:
            logging.error(f"    ❌ Erreur: {str(e)[:100]}")
    
    return False, None

def main():
    logging.info("🔍 DÉMARRAGE DU TEST DES 33 CLÉS API GEMINI")
    logging.info("📋 Versions API disponibles: v1, v2, v2beta, v2internal, v3, v3beta")
    logging.info("📝 Version du script: 7.3\n")
    
    # Charger les clés depuis les variables d'environnement
    api_keys = []
    for i in range(1, 34):  # Tester jusqu'à 33 clés
        key = os.environ.get(f'GOOGLE_API_KEY_{i}')
        if key:
            api_keys.append((i, key))
    
    if not api_keys:
        logging.error("❌ Aucune clé API trouvée dans les variables d'environnement")
        logging.error("   Vérifiez que GOOGLE_API_KEY_1, GOOGLE_API_KEY_2, etc. sont définis")
        logging.error("\n💡 Pour définir les variables:")
        logging.error("   1. Créez un fichier .env")
        logging.error("   2. Ajoutez: export GOOGLE_API_KEY_1='votre_clé'")
        logging.error("   3. Chargez: source .env")
        return
    
    logging.info(f"📊 {len(api_keys)} clé(s) API trouvée(s) sur 33 possibles\n")
    
    if len(api_keys) < 33:
        logging.warning(f"⚠️  Attention: Seulement {len(api_keys)} clés trouvées sur 33 attendues")
        logging.warning(f"   Clés manquantes: {33 - len(api_keys)}")
        logging.warning(f"   Pour performances optimales, configurez toutes les 33 clés\n")
    
    working_keys = []
    failed_keys = []
    
    for key_num, key in api_keys:
        success, working_config = test_gemini_api_key(key, key_num)
        
        if success:
            working_keys.append((key_num, working_config))
        else:
            failed_keys.append(key_num)
        
        if key_num < len(api_keys):
            logging.info("\n⏳ Pause de 2 secondes avant le test suivant...")
            time.sleep(2)
    
    # Résumé final
    logging.info("\n" + "="*60)
    logging.info("📊 RÉSUMÉ DES TESTS")
    logging.info("="*60)
    logging.info(f"✅ Clés fonctionnelles : {len(working_keys)}/{len(api_keys)}")
    
    # Statistiques de performance
    if len(api_keys) == 33:
        logging.info(f"🎯 Configuration optimale: 33/33 clés testées")
    elif len(api_keys) >= 20:
        logging.info(f"✅ Configuration bonne: {len(api_keys)}/33 clés testées")
    elif len(api_keys) >= 10:
        logging.info(f"⚠️  Configuration minimale: {len(api_keys)}/33 clés testées")
    else:
        logging.info(f"❌ Configuration insuffisante: {len(api_keys)}/33 clés testées")
    
    if working_keys:
        logging.info(f"\n🎉 Clés API fonctionnelles:")
        for key_num, config in working_keys[:5]:  # Afficher les 5 premières
            logging.info(f"   • Clé #{key_num} : Modèle '{config['model']}' (API {config['version']})")
        
        if len(working_keys) > 5:
            logging.info(f"   ... et {len(working_keys) - 5} autres clés fonctionnelles")
        
        logging.info(f"\n💡 RECOMMANDATION:")
        recommended_config = working_keys[0][1]
        logging.info(f"   Modèle à utiliser : {recommended_config['model']}")
        logging.info(f"   Version API : {recommended_config['version']}")
        
        # Afficher la configuration à copier dans le code
        logging.info(f"\n📝 Configuration à utiliser dans votre code Python:")
        logging.info(f"   GEMINI_MODEL = \"{recommended_config['model']}\"")
        logging.info(f"   GEMINI_API_VERSION = \"{recommended_config['version']}\"")
        logging.info(f"\n📋 URL API:")
        logging.info(f"   https://generativelanguage.googleapis.com/{recommended_config['version']}/models/{recommended_config['model']}:generateContent?key={{api_key}}")
        
        # Estimation de capacité
        logging.info(f"\n📈 CAPACITÉ DE TRAITEMENT:")
        logging.info(f"   • Requêtes/minute: {len(working_keys) * 15} req/min")
        logging.info(f"   • Requêtes/heure: {len(working_keys) * 15 * 60:,} req/h")
        logging.info(f"   • Requêtes/jour: {len(working_keys) * 1500:,} req/jour")
        
        if len(working_keys) >= 30:
            logging.info(f"   🚀 Performance MAXIMALE - Capacité excellente!")
        elif len(working_keys) >= 20:
            logging.info(f"   ✅ Performance ÉLEVÉE - Capacité très bonne")
        elif len(working_keys) >= 10:
            logging.info(f"   ✔️  Performance STANDARD - Capacité suffisante")
        else:
            logging.info(f"   ⚠️  Performance LIMITÉE - Ajoutez plus de clés")
    
    if failed_keys:
        logging.warning(f"\n⚠️  Clés non fonctionnelles : {failed_keys}")
        logging.warning(f"\n🔧 ACTIONS CORRECTIVES:")
        logging.warning(f"   1. Vérifiez que les clés sont créées sur: https://aistudio.google.com/app/apikey")
        logging.warning(f"   2. Activez l'API 'Generative Language API' dans Google Cloud Console")
        logging.warning(f"   3. Assurez-vous que les clés n'ont pas de restrictions d'API")
        logging.warning(f"   4. Vérifiez que le quota n'est pas dépassé")
        logging.warning(f"   5. Attendez quelques minutes après création de la clé")
        logging.warning(f"\n   Pour recréer les clés en erreur:")
        for key_num in failed_keys[:3]:  # Montrer les 3 premières
            logging.warning(f"   - Clé #{key_num}: Supprimez et recréez sur Google AI Studio")
    
    if not working_keys:
        logging.error(f"\n❌ AUCUNE CLÉ FONCTIONNELLE")
        logging.error(f"   Le système ne pourra pas effectuer d'analyses fondamentales")
        logging.error(f"   Corrigez les clés API avant de continuer")
        logging.error(f"\n   Étapes de résolution:")
        logging.error(f"   1. Vérifiez votre connexion Internet")
        logging.error(f"   2. Allez sur https://aistudio.google.com/app/apikey")
        logging.error(f"   3. Créez de nouvelles clés API")
        logging.error(f"   4. Vérifiez que l'API Generative Language est activée")
        logging.error(f"   5. Relancez ce test après 5 minutes")
    else:
        percentage = (len(working_keys) / len(api_keys)) * 100
        logging.info(f"\n✅ TAUX DE SUCCÈS : {percentage:.1f}%")
        
        if percentage == 100:
            logging.info(f"🎉 PARFAIT ! Toutes les clés fonctionnent")
            logging.info(f"✅ VOUS POUVEZ LANCER LE WORKFLOW GITHUB ACTIONS")
        elif percentage >= 90:
            logging.info(f"✅ EXCELLENT ! Presque toutes les clés fonctionnent")
            logging.info(f"✅ VOUS POUVEZ LANCER LE WORKFLOW GITHUB ACTIONS")
        elif percentage >= 70:
            logging.info(f"✅ BIEN ! La majorité des clés fonctionnent")
            logging.info(f"💡 Recommandation : Corrigez les clés en erreur pour performances optimales")
            logging.info(f"✅ VOUS POUVEZ LANCER LE WORKFLOW GITHUB ACTIONS")
        elif percentage >= 50:
            logging.warning(f"⚠️  MOYEN ! Seulement {percentage:.0f}% des clés fonctionnent")
            logging.warning(f"💡 Recommandation : Corrigez au moins 70% des clés avant de lancer")
        else:
            logging.error(f"❌ INSUFFISANT ! Moins de 50% des clés fonctionnent")
            logging.error(f"❌ NE LANCEZ PAS LE WORKFLOW - Corrigez d'abord les clés")
    
    # Informations complémentaires
    logging.info(f"\n" + "="*60)
    logging.info(f"📚 INFORMATIONS COMPLÉMENTAIRES")
    logging.info(f"="*60)
    logging.info(f"🔗 Ressources utiles:")
    logging.info(f"   • Google AI Studio: https://aistudio.google.com/app/apikey")
    logging.info(f"   • Documentation API: https://ai.google.dev/gemini-api/docs")
    logging.info(f"   • Guide configuration: Voir CONFIGURATION_33_CLES.md")
    
    logging.info(f"\n💾 Configuration GitHub Secrets:")
    logging.info(f"   Pour chaque clé fonctionnelle, créez un secret:")
    logging.info(f"   Settings → Secrets and variables → Actions → New secret")
    logging.info(f"   Name: GOOGLE_API_KEY_X (où X = 1 à 33)")
    logging.info(f"   Value: [Votre clé API]")
    
    logging.info(f"\n⏭️  PROCHAINES ÉTAPES:")
    if len(working_keys) >= len(api_keys) * 0.7:
        logging.info(f"   1. ✅ Les clés sont prêtes")
        logging.info(f"   2. Ajoutez-les dans GitHub Secrets")
        logging.info(f"   3. Lancez le workflow GitHub Actions")
        logging.info(f"   4. Vérifiez les logs de l'étape 4 (Analyse fondamentale)")
    else:
        logging.info(f"   1. ⚠️  Corrigez les clés en erreur: {failed_keys}")
        logging.info(f"   2. Relancez ce test: python test_gemini_api.py")
        logging.info(f"   3. Une fois >70% OK, ajoutez dans GitHub Secrets")
        logging.info(f"   4. Lancez le workflow GitHub Actions")
    
    logging.info(f"\n" + "="*60)
    
    # Code de sortie
    if len(working_keys) >= len(api_keys) * 0.7:
        logging.info(f"✅ TEST RÉUSSI - Système prêt à déployer")
        return 0
    else:
        logging.warning(f"⚠️  TEST PARTIEL - Corrections nécessaires")
        return 1

if __name__ == "__main__":
    import sys
    exit_code = main()
    sys.exit(exit_code)
