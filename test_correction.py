#!/usr/bin/env python3
# ==============================================================================
# SCRIPT DE TEST RAPIDE - VÉRIFICATION CORRECTION 404
# ==============================================================================
# Ce script teste si la correction du modèle Gemini fonctionne
# ==============================================================================

import os
import requests
import sys

# ✅ CONFIGURATION CORRIGÉE
GEMINI_MODEL = "gemini-1.5-flash"  # Sans suffixe "-latest"
GEMINI_API_VERSION = "v1beta"  # Version recommandée pour Gemini 1.5

def test_single_key():
    """Test rapide avec une seule clé"""
    
    # Chercher la première clé disponible
    api_key = None
    for i in range(1, 51):
        key = os.environ.get(f'GOOGLE_API_KEY_{i}')
        if key:
            api_key = key.strip()
            print(f"✅ Clé #{i} trouvée")
            break
    
    if not api_key:
        print("❌ Aucune clé API trouvée dans les variables d'environnement")
        print("\n💡 Configurez au moins une clé :")
        print("   export GOOGLE_API_KEY_1='votre_clé'")
        return False
    
    # Masquer la clé
    masked_key = api_key[:8] + "..." + api_key[-4:]
    print(f"🔑 Test avec clé : {masked_key}")
    
    # URL corrigée
    api_url = f"https://generativelanguage.googleapis.com/{GEMINI_API_VERSION}/models/{GEMINI_MODEL}:generateContent"
    
    print(f"\n📡 URL testée :")
    print(f"   {api_url}")
    
    # Headers corrigés
    headers = {
        "Content-Type": "application/json",
        "x-goog-api-key": api_key
    }
    
    # Requête de test simple
    test_request = {
        "contents": [{
            "parts": [{"text": "Dis bonjour en une phrase"}]
        }],
        "generationConfig": {
            "temperature": 0.7,
            "maxOutputTokens": 50
        }
    }
    
    print("\n⏳ Envoi de la requête...")
    
    try:
        response = requests.post(api_url, headers=headers, json=test_request, timeout=15)
        
        print(f"\n📊 Code de réponse : {response.status_code}")
        
        if response.status_code == 200:
            print("\n🎉 SUCCÈS ! La correction fonctionne !")
            
            try:
                result = response.json()
                answer = result['candidates'][0]['content']['parts'][0]['text']
                print(f"\n💬 Réponse de l'API :")
                print(f"   {answer}")
                
                print("\n" + "="*60)
                print("✅ TOUT FONCTIONNE CORRECTEMENT")
                print("="*60)
                print("📋 Configuration validée :")
                print(f"   • Modèle : {GEMINI_MODEL}")
                print(f"   • Version API : {GEMINI_API_VERSION}")
                print(f"   • Clé API : Valide")
                print("\n👉 Vous pouvez déployer les fichiers corrigés sur GitHub")
                return True
                
            except Exception as e:
                print(f"⚠️  Réponse reçue mais format inattendu : {e}")
                print(f"Réponse brute : {response.text[:300]}")
                return False
        
        elif response.status_code == 404:
            print("\n❌ ERREUR 404 - Le problème persiste")
            print("\n🔍 Diagnostic :")
            try:
                error_detail = response.json()
                error_msg = error_detail.get('error', {}).get('message', '')
                print(f"   Message : {error_msg}")
            except:
                print(f"   Réponse : {response.text[:200]}")
            
            print("\n💡 Vérifications à faire :")
            print("   1. Le modèle est-il bien 'gemini-1.5-flash' (sans -latest) ?")
            print("   2. La version API est-elle 'v1beta' ?")
            print("   3. Avez-vous remplacé les fichiers fundamental_analyzer.py et report_generator.py ?")
            return False
        
        elif response.status_code == 403:
            print("\n❌ ERREUR 403 - Accès refusé")
            print("\n💡 Causes possibles :")
            print("   • API 'Generative Language API' pas activée dans Google Cloud")
            print("   • Clé API invalide ou expirée")
            print("   • Restrictions sur la clé API")
            print("\n🔗 Activez l'API ici :")
            print("   https://console.cloud.google.com/apis/library/generativelanguage.googleapis.com")
            return False
        
        elif response.status_code == 429:
            print("\n⚠️  ERREUR 429 - Quota dépassé")
            print("   La clé API fonctionne mais le quota est atteint")
            print("   Essayez avec une autre clé ou attendez la réinitialisation du quota")
            return False
        
        else:
            print(f"\n❌ Erreur inattendue : {response.status_code}")
            print(f"   Réponse : {response.text[:300]}")
            return False
    
    except requests.exceptions.Timeout:
        print("\n❌ Timeout - Le serveur ne répond pas")
        print("   Vérifiez votre connexion internet")
        return False
    
    except requests.exceptions.ConnectionError:
        print("\n❌ Erreur de connexion")
        print("   Impossible de joindre les serveurs Google")
        return False
    
    except Exception as e:
        print(f"\n❌ Erreur : {str(e)}")
        return False

def main():
    print("="*60)
    print("🔬 TEST RAPIDE - VÉRIFICATION CORRECTION 404")
    print("="*60)
    print(f"\nConfiguration testée :")
    print(f"  • Modèle : {GEMINI_MODEL}")
    print(f"  • Version API : {GEMINI_API_VERSION}")
    print()
    
    success = test_single_key()
    
    if success:
        sys.exit(0)
    else:
        print("\n" + "="*60)
        print("⚠️  DES PROBLÈMES ONT ÉTÉ DÉTECTÉS")
        print("="*60)
        print("\n📖 Consultez CORRECTION_404_README.md pour plus d'aide")
        sys.exit(1)

if __name__ == "__main__":
    main()
