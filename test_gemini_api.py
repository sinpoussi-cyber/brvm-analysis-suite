# ==============================================================================
# SCRIPT DE TEST - VÉRIFICATION DES CLÉS API GEMINI (VERSION CORRIGÉE)
# ==============================================================================

import os
import requests
import logging
import time

GEMINI_MODEL = os.environ.get("GEMINI_MODEL", "gemini-1.5-flash")
GEMINI_API_VERSION = os.environ.get("GEMINI_API_VERSION", "v1beta")


def build_gemini_url(model: str, version: str) -> str:
    """Construit l'URL pour appeler un modèle Gemini."""

    clean_model = model.strip()
    clean_version = version.strip()
    return (
        f"https://generativelanguage.googleapis.com/"
        f"{clean_version}/models/{clean_model}:generateContent"
    )

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s')

def test_gemini_api_key(api_key, key_number):
    """Teste une clé API Gemini avec la configuration corrigée"""
    logging.info(f"\n{'='*60}")
    logging.info(f"TEST DE LA CLÉ API #{key_number}")
    logging.info(f"{'='*60}")
    
    # Nettoyer la clé
    api_key = api_key.strip()
    
    # Masquer la clé pour la sécurité
    masked_key = api_key[:8] + "..." + api_key[-4:] if len(api_key) > 12 else "***"
    logging.info(f"Clé : {masked_key}")
    
    # ✅ CONFIGURATION CORRIGÉE
    test_configs = [
        # Option 1 : configuration par défaut basée sur les variables d'environnement
        {
            "url": build_gemini_url(GEMINI_MODEL, GEMINI_API_VERSION),
            "model": GEMINI_MODEL,
            "version": GEMINI_API_VERSION,
            "use_header": True,  # Utiliser x-goog-api-key dans le header
        },
        # Option 2 : v1beta avec gemini-1.5-flash
        {
            "url": build_gemini_url("gemini-1.5-flash", "v1beta"),
            "model": "gemini-1.5-flash",
            "version": "v1beta",
            "use_header": True,
        },
        # Option 3 : v1beta avec gemini-1.5-pro
        {
            "url": build_gemini_url("gemini-1.5-pro", "v1beta"),
            "model": "gemini-1.5-pro",
            "version": "v1beta",
            "use_header": True,
        },
        # Option 4 : v1 avec gemini-pro (plus ancien mais stable)
        {
            "url": "https://generativelanguage.googleapis.com/v1/models/gemini-pro:generateContent",
            "model": "gemini-pro",
            "version": "v1",
            "use_header": True
        },
    ]
    
    # Requête de test simple
    test_request = {
        "contents": [{
            "parts": [{"text": "Dis bonjour en français en une phrase"}]
        }],
        "generationConfig": {
            "temperature": 0.7,
            "maxOutputTokens": 50
        }
    }
    
    for idx, config in enumerate(test_configs, 1):
        url = config["url"]
        model_name = config["model"]
        api_version = config["version"]
        use_header = config["use_header"]
        
        logging.info(f"\n  Test {idx}/{len(test_configs)} : Modèle '{model_name}' (API {api_version})")
        
        try:
            # ✅ HEADERS CORRIGÉS
            if use_header:
                headers = {
                    "Content-Type": "application/json",
                    "x-goog-api-key": api_key
                }
                test_url = url
            else:
                headers = {"Content-Type": "application/json"}
                test_url = f"{url}?key={api_key}"
            
            response = requests.post(test_url, headers=headers, json=test_request, timeout=15)
            
            if response.status_code == 200:
                logging.info(f"    ✅ SUCCÈS ! Ce modèle fonctionne")
                try:
                    result = response.json()
                    answer = result['candidates'][0]['content']['parts'][0]['text']
                    logging.info(f"    📝 Réponse: {answer[:80]}...")
                    return True, config
                except Exception as e:
                    logging.info(f"    ⚠️  Réponse reçue mais format inattendu: {e}")
                    
            elif response.status_code == 404:
                error_detail = ""
                try:
                    error_detail = response.json().get("error", {}).get("message", "")
                except ValueError:
                    error_detail = response.text[:200]

                logging.warning("    ❌ 404 - Modèle ou endpoint non trouvé")
                if error_detail:
                    logging.warning(f"       Détail: {error_detail}")
                logging.warning(f"       URL testée: {test_url[:80]}...")
                
            elif response.status_code == 403:
                logging.error(f"    ❌ 403 - Accès refusé. Vérifiez:")
                logging.error(f"       • Clé API valide?")
                logging.error(f"       • API Generative Language activée dans GCP?")
                logging.error(f"       • Restrictions sur la clé?")
                
            elif response.status_code == 429:
                logging.warning(f"    ⚠️  429 - Quota dépassé")
                
            elif response.status_code == 400:
                logging.error(f"    ❌ 400 - Requête invalide")
                logging.error(f"       Réponse: {response.text[:200]}")
                
            else:
                logging.warning(f"    ❌ Erreur {response.status_code}")
                logging.warning(f"       Réponse: {response.text[:200]}")
                
        except requests.exceptions.Timeout:
            logging.error(f"    ❌ Timeout - Le serveur ne répond pas")
        except requests.exceptions.ConnectionError:
            logging.error(f"    ❌ Erreur de connexion")
        except Exception as e:
            logging.error(f"    ❌ Erreur: {str(e)[:150]}")
    
    return False, None

def main():
    logging.info("🔍 DÉMARRAGE DU TEST DES CLÉS API GEMINI (VERSION CORRIGÉE)")
    logging.info("📋 Configuration: v1beta avec x-goog-api-key header")
    logging.info("📝 Version du script: 7.4\n")
    
    # Charger les clés
    api_keys = []
    for i in range(1, 34):
        key = os.environ.get(f'GOOGLE_API_KEY_{i}')
        if key:
            api_keys.append((i, key.strip()))
    
    if not api_keys:
        logging.error("❌ Aucune clé API trouvée dans les variables d'environnement")
        logging.error("\n💡 Configuration requise:")
        logging.error("   export GOOGLE_API_KEY_1='votre_clé'")
        logging.error("   export GOOGLE_API_KEY_2='votre_clé'")
        logging.error("   etc.")
        return
    
    logging.info(f"📊 {len(api_keys)} clé(s) API trouvée(s)\n")
    
    working_keys = []
    failed_keys = []
    
    for key_num, key in api_keys:
        success, working_config = test_gemini_api_key(key, key_num)
        
        if success:
            working_keys.append((key_num, working_config))
        else:
            failed_keys.append(key_num)
        
        if key_num < len(api_keys):
            logging.info("\n⏳ Pause de 2 secondes...")
            time.sleep(2)
    
    # Résumé
    logging.info("\n" + "="*60)
    logging.info("📊 RÉSUMÉ DES TESTS")
    logging.info("="*60)
    logging.info(f"✅ Clés fonctionnelles : {len(working_keys)}/{len(api_keys)}")
    
    if working_keys:
        logging.info(f"\n🎉 Clés API fonctionnelles:")
        for key_num, config in working_keys[:5]:
            logging.info(f"   • Clé #{key_num} : Modèle '{config['model']}' (API {config['version']})")
        
        if len(working_keys) > 5:
            logging.info(f"   ... et {len(working_keys) - 5} autres")
        
        recommended = working_keys[0][1]
        logging.info(f"\n💡 CONFIGURATION RECOMMANDÉE:")
        logging.info(f"   GEMINI_MODEL = \"{recommended['model']}\"")
        logging.info(f"   GEMINI_API_VERSION = \"{recommended['version']}\"")
        logging.info(f"\n📋 Format de requête:")
        logging.info(f"   URL: {recommended['url']}")
        logging.info("   Headers: {'Content-Type': 'application/json', 'x-goog-api-key': '<YOUR_KEY>'}")

        logging.info(f"\n📈 CAPACITÉ:")
        logging.info(f"   • {len(working_keys) * 15} requêtes/minute")
        logging.info(f"   • {len(working_keys) * 1500:,} requêtes/jour")
    
    if failed_keys:
        logging.warning(f"\n⚠️  Clés en erreur : {failed_keys}")
        logging.warning(f"\n🔧 VÉRIFICATIONS:")
        logging.warning(f"   1. Clé créée sur: https://aistudio.google.com/app/apikey")
        logging.warning(f"   2. API 'Generative Language API' activée dans GCP")
        logging.warning(f"   3. Pas de restrictions d'API sur la clé")
        logging.warning(f"   4. Quota non dépassé")
    
    percentage = (len(working_keys) / len(api_keys) * 100) if api_keys else 0
    logging.info(f"\n✅ TAUX DE SUCCÈS : {percentage:.1f}%")
    
    if percentage >= 70:
        logging.info(f"✅ SYSTÈME PRÊT - Vous pouvez lancer le workflow")
        return 0
    else:
        logging.warning(f"⚠️  CORRECTIONS NÉCESSAIRES - Corrigez les clés en erreur")
        return 1

if __name__ == "__main__":
    import sys
    exit_code = main()
    sys.exit(exit_code)
