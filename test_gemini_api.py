# ==============================================================================
# SCRIPT DE TEST - VÉRIFICATION DES CLÉS API GEMINI (VERSION CORRIGÉE)
# ==============================================================================

import os
import requests
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s')

def test_gemini_api_key(api_key, key_number):
    """
    Teste une clé API Gemini avec une requête simple
    """
    logging.info(f"\n{'='*60}")
    logging.info(f"TEST DE LA CLÉ API #{key_number}")
    logging.info(f"{'='*60}")
    
    # Masquer la clé pour la sécurité
    masked_key = api_key[:8] + "..." + api_key[-4:] if len(api_key) > 12 else "***"
    logging.info(f"Clé : {masked_key}")
    
    # URLs à tester (dans l'ordre de priorité)
    test_urls = [
        # ✅ Option 1 : Gemini 1.5 Flash Latest (RECOMMANDÉ - API v1)
        {
            "url": f"https://generativelanguage.googleapis.com/v1/models/gemini-1.5-flash-latest:generateContent?key={api_key}",
            "model": "gemini-1.5-flash-latest",
            "version": "v1"
        },
        # Option 2 : Gemini 1.5 Pro (API v1)
        {
            "url": f"https://generativelanguage.googleapis.com/v1/models/gemini-1.5-pro:generateContent?key={api_key}",
            "model": "gemini-1.5-pro",
            "version": "v1"
        },
        # Option 3 : Gemini 1.5 Flash (API v1)
        {
            "url": f"https://generativelanguage.googleapis.com/v1/models/gemini-1.5-flash:generateContent?key={api_key}",
            "model": "gemini-1.5-flash",
            "version": "v1"
        },
        # Option 4 : Gemini 1.5 Flash Latest (API v1beta - ancienne version)
        {
            "url": f"https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash-latest:generateContent?key={api_key}",
            "model": "gemini-1.5-flash-latest",
            "version": "v1beta"
        },
        # Option 5 : Gemini 1.5 Flash (API v1beta - ancienne version)
        {
            "url": f"https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent?key={api_key}",
            "model": "gemini-1.5-flash",
            "version": "v1beta"
        },
    ]
    
    # Test simple
    test_request = {
        "contents": [{
            "parts": [{"text": "Dis bonjour en français"}]
        }]
    }
    
    headers = {"Content-Type": "application/json"}
    
    for idx, test_config in enumerate(test_urls, 1):
        url = test_config["url"]
        model_name = test_config["model"]
        api_version = test_config["version"]
        
        logging.info(f"\n  Test {idx}/{len(test_urls)} : Modèle '{model_name}' (API {api_version})")
        
        try:
            response = requests.post(url, json=test_request, headers=headers, timeout=10)
            
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
                logging.warning(f"    ❌ 404 - Modèle non trouvé ou API non activée")
                
            elif response.status_code == 403:
                logging.error(f"    ❌ 403 - Accès refusé. Vérifiez les permissions de la clé")
                
            elif response.status_code == 429:
                logging.warning(f"    ⚠️  429 - Quota dépassé")
                
            else:
                logging.warning(f"    ❌ Erreur {response.status_code}: {response.text[:100]}")
                
        except requests.exceptions.Timeout:
            logging.error(f"    ❌ Timeout - Le serveur ne répond pas")
        except Exception as e:
            logging.error(f"    ❌ Erreur: {e}")
    
    return False, None

def main():
    logging.info("🔍 DÉMARRAGE DU TEST DES CLÉS API GEMINI (VERSION CORRIGÉE)\n")
    
    # Charger les clés depuis les variables d'environnement
    api_keys = []
    for i in range(1, 23):  # Tester jusqu'à 22 clés
        key = os.environ.get(f'GOOGLE_API_KEY_{i}')
        if key:
            api_keys.append(key)
    
    if not api_keys:
        logging.error("❌ Aucune clé API trouvée dans les variables d'environnement")
        logging.error("   Vérifiez que GOOGLE_API_KEY_1, GOOGLE_API_KEY_2, etc. sont définis")
        return
    
    logging.info(f"📊 {len(api_keys)} clé(s) API trouvée(s)\n")
    
    working_keys = []
    failed_keys = []
    
    for idx, key in enumerate(api_keys, 1):
        success, working_config = test_gemini_api_key(key, idx)
        
        if success:
            working_keys.append((idx, working_config))
        else:
            failed_keys.append(idx)
        
        if idx < len(api_keys):
            logging.info("\n⏳ Pause de 2 secondes avant le test suivant...")
            import time
            time.sleep(2)
    
    # Résumé final
    logging.info("\n" + "="*60)
    logging.info("📊 RÉSUMÉ DES TESTS")
    logging.info("="*60)
    logging.info(f"✅ Clés fonctionnelles : {len(working_keys)}/{len(api_keys)}")
    
    if working_keys:
        logging.info("\n🎉 Clés API fonctionnelles:")
        for key_num, config in working_keys:
            logging.info(f"   • Clé #{key_num} : Modèle '{config['model']}' (API {config['version']})")
        
        logging.info("\n💡 RECOMMANDATION:")
        recommended_config = working_keys[0][1]
        logging.info(f"   Modèle à utiliser : {recommended_config['model']}")
        logging.info(f"   Version API : {recommended_config['version']}")
        
        # Afficher la configuration à copier dans le code
        logging.info(f"\n📝 Configuration à utiliser dans votre code Python:")
        logging.info(f"   GEMINI_MODEL = \"{recommended_config['model']}\"")
        logging.info(f"   GEMINI_API_VERSION = \"{recommended_config['version']}\"")
        logging.info(f"   API_URL = \"https://generativelanguage.googleapis.com/{recommended_config['version']}/models/{{GEMINI_MODEL}}:generateContent?key={{api_key}}\"")
        
    if failed_keys:
        logging.warning(f"\n⚠️  Clés non fonctionnelles : {failed_keys}")
        logging.warning("\n🔧 ACTIONS CORRECTIVES:")
        logging.warning("   1. Vérifiez que les clés sont créées sur: https://aistudio.google.com/app/apikey")
        logging.warning("   2. Activez l'API 'Generative Language API' dans Google Cloud Console")
        logging.warning("   3. Assurez-vous que les clés n'ont pas de restrictions d'API")
        logging.warning("   4. Vérifiez que le quota n'est pas dépassé")
    
    if not working_keys:
        logging.error("\n❌ AUCUNE CLÉ FONCTIONNELLE")
        logging.error("   Le système ne pourra pas effectuer d'analyses fondamentales")
        logging.error("   Corrigez les clés API avant de continuer")
    else:
        logging.info("\n✅ AU MOINS UNE CLÉ FONCTIONNE - VOUS POUVEZ LANCER LE WORKFLOW")
    
    logging.info("="*60)

if __name__ == "__main__":
    main()
