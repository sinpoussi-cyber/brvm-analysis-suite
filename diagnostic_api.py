#!/usr/bin/env python3
# ==============================================================================
# SCRIPT DE DIAGNOSTIC AUTOMATIQUE - API GEMINI (VERSION 50 CLÉS)
# ==============================================================================
# Ce script diagnostique automatiquement les problèmes avec l'API Gemini
# et propose des solutions concrètes.
# Version: 9.1 - Support de 50 clés API
# ==============================================================================

import os
import sys
import requests
import json
import re
from datetime import datetime


GEMINI_MODEL = os.environ.get("GEMINI_MODEL", "gemini-1.5-flash")
GEMINI_API_VERSION = os.environ.get("GEMINI_API_VERSION", "v1beta")


def build_gemini_url(model: str) -> str:
    """Retourne l'URL complète pour le modèle Gemini donné."""

    clean_model = model.strip()
    return (
        f"https://generativelanguage.googleapis.com/"
        f"{GEMINI_API_VERSION}/models/{clean_model}:generateContent"
    )

class Colors:
    """Codes couleur pour terminal"""
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

def print_header(text):
    """Affiche un en-tête formaté"""
    print(f"\n{Colors.HEADER}{'='*60}{Colors.ENDC}")
    print(f"{Colors.HEADER}{text}{Colors.ENDC}")
    print(f"{Colors.HEADER}{'='*60}{Colors.ENDC}\n")

def print_success(text):
    """Affiche un message de succès"""
    print(f"{Colors.OKGREEN}✅ {text}{Colors.ENDC}")

def print_warning(text):
    """Affiche un avertissement"""
    print(f"{Colors.WARNING}⚠️  {text}{Colors.ENDC}")

def print_error(text):
    """Affiche une erreur"""
    print(f"{Colors.FAIL}❌ {text}{Colors.ENDC}")

def print_info(text):
    """Affiche une information"""
    print(f"{Colors.OKCYAN}ℹ️  {text}{Colors.ENDC}")

class GeminiDiagnostic:
    def __init__(self):
        self.api_keys = []
        self.issues = []
        self.warnings = []
        self.successes = []
        
    def load_api_keys(self):
        """Charge toutes les clés API disponibles (1 à 50)"""
        print_info("Chargement des clés API...")
        for i in range(1, 51):  # ✅ MIS À JOUR pour 50 clés
            key = os.environ.get(f'GOOGLE_API_KEY_{i}')
            if key:
                self.api_keys.append({
                    'number': i,
                    'key': key.strip(),
                    'valid': None,
                    'error': None
                })
        
        if not self.api_keys:
            print_error("Aucune clé API trouvée dans les variables d'environnement")
            self.issues.append("Aucune clé API configurée")
            return False
        
        print_success(f"{len(self.api_keys)} clé(s) API trouvée(s)")
        return True
    
    def check_key_format(self, key):
        """Vérifie le format de la clé API"""
        issues = []
        
        # Longueur attendue (environ 39 caractères)
        if len(key) < 30:
            issues.append("Clé trop courte (< 30 caractères)")
        elif len(key) > 50:
            issues.append("Clé trop longue (> 50 caractères)")
        
        # Caractères suspects
        if '\n' in key or '\r' in key:
            issues.append("Contient des retours à la ligne")
        if ' ' in key:
            issues.append("Contient des espaces")
        
        # Format attendu (alphanumeric + quelques caractères spéciaux)
        if not re.match(r'^[A-Za-z0-9_-]+$', key):
            issues.append("Contient des caractères non autorisés")
        
        return issues
    
    def test_api_endpoint(self, key_data):
        """Teste un endpoint API avec une clé"""
        key = key_data['key']
        
        # Utiliser le modèle et la version configurables
        url = build_gemini_url(GEMINI_MODEL)
        headers = {
            "Content-Type": "application/json",
            "x-goog-api-key": key
        }
        body = {
            "contents": [{
                "parts": [{"text": "Hello"}]
            }],
            "generationConfig": {
                "maxOutputTokens": 10
            }
        }
        
        try:
            response = requests.post(url, headers=headers, json=body, timeout=10)
            
            key_data['status_code'] = response.status_code
            key_data['response_time'] = response.elapsed.total_seconds()
            
            if response.status_code == 200:
                key_data['valid'] = True
                key_data['error'] = None
                return True, "OK"
            
            elif response.status_code == 404:
                key_data['valid'] = False
                key_data['error'] = "404 Not Found"
                return False, "404 - Endpoint ou modèle introuvable"
            
            elif response.status_code == 403:
                key_data['valid'] = False
                key_data['error'] = "403 Forbidden"
                return False, "403 - Clé invalide ou API non activée"
            
            elif response.status_code == 429:
                key_data['valid'] = False
                key_data['error'] = "429 Too Many Requests"
                return False, "429 - Quota dépassé"
            
            elif response.status_code == 400:
                key_data['valid'] = False
                key_data['error'] = "400 Bad Request"
                return False, "400 - Requête invalide"
            
            else:
                key_data['valid'] = False
                key_data['error'] = f"{response.status_code}"
                return False, f"Erreur {response.status_code}"
        
        except requests.exceptions.Timeout:
            key_data['valid'] = False
            key_data['error'] = "Timeout"
            return False, "Timeout - Serveur ne répond pas"
        
        except requests.exceptions.ConnectionError:
            key_data['valid'] = False
            key_data['error'] = "Connection Error"
            return False, "Erreur de connexion réseau"
        
        except Exception as e:
            key_data['valid'] = False
            key_data['error'] = str(e)
            return False, f"Exception: {str(e)}"
    
    def diagnose_files(self):
        """Vérifie la configuration des fichiers Python"""
        print_header("Diagnostic des Fichiers")
        
        files_to_check = {
            'fundamental_analyzer.py': {
                'required_strings': [
                    'GEMINI_API_VERSION',
                    'x-goog-api-key',
                    'api_key.strip()'
                ],
                'deprecated_strings': [
                    'v2beta',
                    '?key='
                ]
            },
            'report_generator.py': {
                'required_strings': [
                    'GEMINI_API_VERSION',
                    'x-goog-api-key',
                    'api_key.strip()'
                ],
                'deprecated_strings': [
                    'v2beta',
                    '?key='
                ]
            }
        }
        
        for filename, checks in files_to_check.items():
            print(f"\n📄 Vérification de {filename}...")
            
            if not os.path.exists(filename):
                print_warning(f"Fichier {filename} introuvable")
                self.warnings.append(f"{filename} manquant")
                continue
            
            with open(filename, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # Vérifier les chaînes requises
            all_found = True
            for required in checks['required_strings']:
                if required in content:
                    print_success(f"Trouvé: {required}")
                else:
                    print_error(f"Manquant: {required}")
                    self.issues.append(f"{filename}: {required} manquant")
                    all_found = False
            
            # Vérifier les chaînes obsolètes
            found_deprecated = False
            for deprecated in checks['deprecated_strings']:
                if deprecated in content:
                    print_error(f"Trouvé (obsolète): {deprecated}")
                    self.issues.append(f"{filename}: {deprecated} obsolète trouvé")
                    found_deprecated = True
            
            if all_found and not found_deprecated:
                print_success(f"{filename} correctement configuré")
                self.successes.append(f"{filename} OK")
    
    def diagnose_api_keys(self):
        """Diagnostique toutes les clés API"""
        print_header("Diagnostic des Clés API (50 clés)")
        
        if not self.api_keys:
            print_error("Aucune clé à diagnostiquer")
            return
        
        valid_keys = 0
        
        for key_data in self.api_keys:
            key_num = key_data['number']
            key = key_data['key']
            
            print(f"\n🔑 Test de la clé #{key_num}")
            print(f"   Aperçu: {key[:8]}...{key[-4:]}")
            
            # Vérifier le format
            format_issues = self.check_key_format(key)
            if format_issues:
                print_warning(f"   Problèmes de format:")
                for issue in format_issues:
                    print(f"     • {issue}")
                self.warnings.append(f"Clé #{key_num}: {', '.join(format_issues)}")
            
            # Tester l'API
            success, message = self.test_api_endpoint(key_data)
            
            if success:
                print_success(f"   {message}")
                print_info(f"   Temps de réponse: {key_data['response_time']:.2f}s")
                valid_keys += 1
                self.successes.append(f"Clé #{key_num} fonctionnelle")
            else:
                print_error(f"   {message}")
                self.issues.append(f"Clé #{key_num}: {message}")
        
        print(f"\n{'='*60}")
        percentage = (valid_keys / len(self.api_keys) * 100) if self.api_keys else 0
        
        if percentage == 100:
            print_success(f"Toutes les clés fonctionnent ({valid_keys}/{len(self.api_keys)})")
        elif percentage >= 70:
            print_success(f"{valid_keys}/{len(self.api_keys)} clés fonctionnent ({percentage:.0f}%)")
        elif percentage >= 50:
            print_warning(f"Seulement {valid_keys}/{len(self.api_keys)} clés fonctionnent ({percentage:.0f}%)")
        else:
            print_error(f"Trop peu de clés fonctionnent: {valid_keys}/{len(self.api_keys)} ({percentage:.0f}%)")
        
        return valid_keys, len(self.api_keys)
    
    def check_google_cloud_config(self):
        """Vérifie la configuration Google Cloud (via API)"""
        print_header("Vérification Configuration Google Cloud")
        
        if not self.api_keys:
            print_error("Aucune clé API pour tester")
            return
        
        # Prendre la première clé pour tester
        test_key = self.api_keys[0]['key']
        
        # Tester la liste des modèles disponibles
        print_info("Test de l'accès aux modèles...")
        url = f"https://generativelanguage.googleapis.com/{GEMINI_API_VERSION}/models"
        headers = {"x-goog-api-key": test_key}
        
        try:
            response = requests.get(url, headers=headers, timeout=10)
            
            if response.status_code == 200:
                models = response.json().get('models', [])
                gemini_models = [m for m in models if 'gemini' in m.get('name', '').lower()]
                
                print_success(f"Accès aux modèles OK")
                print_info(f"   {len(gemini_models)} modèle(s) Gemini disponible(s)")
                
                # Afficher les modèles disponibles
                for model in gemini_models[:5]:
                    model_name = model.get('name', 'N/A').split('/')[-1]
                    print(f"     • {model_name}")
                
                self.successes.append("Accès API Google Cloud OK")
            
            elif response.status_code == 403:
                print_error("API Generative Language pas activée dans Google Cloud")
                print_info("   👉 Solution: https://console.cloud.google.com/apis/library/generativelanguage.googleapis.com")
                self.issues.append("API Generative Language non activée")
            
            else:
                print_warning(f"Réponse inattendue: {response.status_code}")
                self.warnings.append(f"Réponse inattendue lors du test des modèles: {response.status_code}")
        
        except Exception as e:
            print_error(f"Erreur lors du test: {e}")
            self.issues.append(f"Erreur test Google Cloud: {e}")
    
    def check_network_connectivity(self):
        """Vérifie la connectivité réseau vers Google"""
        print_header("Vérification Connectivité Réseau")
        
        endpoints = [
            ("Google DNS", "https://dns.google"),
            ("Google API", "https://generativelanguage.googleapis.com"),
            ("AI Studio", "https://aistudio.google.com")
        ]
        
        for name, url in endpoints:
            print(f"   Test {name}...", end=" ")
            try:
                response = requests.get(url, timeout=5)
                if response.status_code in [200, 301, 302, 404]:  # Accepter redirections et 404
                    print_success("OK")
                else:
                    print_warning(f"Status {response.status_code}")
            except requests.exceptions.Timeout:
                print_error("Timeout")
                self.issues.append(f"Timeout vers {name}")
            except requests.exceptions.ConnectionError:
                print_error("Erreur connexion")
                self.issues.append(f"Pas de connexion vers {name}")
            except Exception as e:
                print_error(f"Erreur: {e}")
    
    def generate_report(self):
        """Génère un rapport de diagnostic complet"""
        print_header("Rapport de Diagnostic")
        
        # Succès
        if self.successes:
            print(f"\n{Colors.OKGREEN}✅ SUCCÈS ({len(self.successes)}){Colors.ENDC}")
            for success in self.successes:
                print(f"   ✓ {success}")
        
        # Avertissements
        if self.warnings:
            print(f"\n{Colors.WARNING}⚠️  AVERTISSEMENTS ({len(self.warnings)}){Colors.ENDC}")
            for warning in self.warnings:
                print(f"   • {warning}")
        
        # Problèmes
        if self.issues:
            print(f"\n{Colors.FAIL}❌ PROBLÈMES ({len(self.issues)}){Colors.ENDC}")
            for issue in self.issues:
                print(f"   ✗ {issue}")
        
        # Recommandations
        print_header("Recommandations")
        
        if not self.issues and not self.warnings:
            print_success("Aucun problème détecté ! Système prêt.")
            print_info("Vous pouvez lancer le workflow GitHub Actions")
            return 0
        
        # Recommandations spécifiques
        if any("404" in issue for issue in self.issues):
            print("\n🔧 PROBLÈME: Erreur 404")
            print("   Causes possibles:")
            print("   1. Version API incorrecte (doit être v1beta)")
            print("   2. URL de l'endpoint incorrecte")
            print("   3. Modèle non disponible")
            print("\n   Solutions:")
            print("   • Vérifier GEMINI_API_VERSION = 'v1beta'")
            print("   • Vérifier que x-goog-api-key est dans les headers")
            print("   • Relancer après correction des fichiers")
        
        if any("403" in issue for issue in self.issues):
            print("\n🔧 PROBLÈME: Erreur 403")
            print("   Causes possibles:")
            print("   1. API Generative Language pas activée")
            print("   2. Clé API invalide")
            print("   3. Restrictions sur la clé")
            print("\n   Solutions:")
            print("   • Activer: https://console.cloud.google.com/apis/library/generativelanguage.googleapis.com")
            print("   • Vérifier les clés sur: https://aistudio.google.com/app/apikey")
            print("   • Retirer les restrictions sur les clés")
        
        if any("manquant" in issue.lower() for issue in self.issues):
            print("\n🔧 PROBLÈME: Fichiers non à jour")
            print("   Solutions:")
            print("   • Télécharger la version 9.1 des fichiers")
            print("   • Remplacer fundamental_analyzer.py")
            print("   • Remplacer report_generator.py")
            print("   • Relancer ce diagnostic")
        
        if any("obsolète" in issue.lower() for issue in self.issues):
            print("\n🔧 PROBLÈME: Configuration obsolète")
            print("   Solutions:")
            print("   • Mettre à jour vers v9.1")
            print("   • Remplacer 'v2beta' par 'v1beta'")
            print("   • Remplacer query param par header x-goog-api-key")
        
        return 1 if self.issues else 0
    
    def save_diagnostic_log(self):
        """Sauvegarde le diagnostic dans un fichier"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"diagnostic_gemini_{timestamp}.json"
        
        report = {
            'timestamp': timestamp,
            'total_keys': len(self.api_keys),
            'valid_keys': sum(1 for k in self.api_keys if k.get('valid')),
            'keys_details': self.api_keys,
            'successes': self.successes,
            'warnings': self.warnings,
            'issues': self.issues
        }
        
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
        
        print_info(f"Rapport sauvegardé dans: {filename}")
        return filename

def main():
    """Fonction principale"""
    print(f"{Colors.BOLD}")
    print("╔════════════════════════════════════════════════════════════╗")
    print("║     DIAGNOSTIC AUTOMATIQUE - API GEMINI v9.1 (50 CLÉS)   ║")
    print("║     Détection et résolution des problèmes                  ║")
    print("╚════════════════════════════════════════════════════════════╝")
    print(f"{Colors.ENDC}")
    
    diag = GeminiDiagnostic()
    
    # Étape 1: Charger les clés
    if not diag.load_api_keys():
        print_error("\n❌ Impossible de continuer sans clés API")
        print_info("Configurez vos clés API avec:")
        print("   export GOOGLE_API_KEY_1='votre_clé'")
        print("   export GOOGLE_API_KEY_2='votre_clé'")
        print("   ... jusqu'à GOOGLE_API_KEY_50")
        return 1
    
    # Étape 2: Vérifier les fichiers
    diag.diagnose_files()
    
    # Étape 3: Tester la connectivité
    diag.check_network_connectivity()
    
    # Étape 4: Vérifier Google Cloud
    diag.check_google_cloud_config()
    
    # Étape 5: Diagnostiquer les clés API
    valid, total = diag.diagnose_api_keys()
    
    # Étape 6: Générer le rapport
    exit_code = diag.generate_report()
    
    # Étape 7: Sauvegarder le log
    log_file = diag.save_diagnostic_log()
    
    # Message final
    print_header("Résumé Final")
    
    percentage = (valid / total * 100) if total > 0 else 0
    
    if exit_code == 0:
        print_success("🎉 DIAGNOSTIC RÉUSSI")
        print_info(f"   • {valid}/{total} clés fonctionnelles ({percentage:.0f}%)")
        print_info(f"   • {len(diag.successes)} vérifications réussies")
        print_info(f"   • Système prêt pour production")
        print_info(f"\n👉 Prochaine étape: Lancer le workflow GitHub Actions")
    else:
        print_error("⚠️  DIAGNOSTIC AVEC PROBLÈMES")
        print_info(f"   • {valid}/{total} clés fonctionnelles ({percentage:.0f}%)")
        print_info(f"   • {len(diag.issues)} problème(s) détecté(s)")
        print_info(f"   • {len(diag.warnings)} avertissement(s)")
        print_info(f"\n👉 Consultez les recommandations ci-dessus")
        print_info(f"👉 Log détaillé: {log_file}")
    
    return exit_code

if __name__ == "__main__":
    try:
        exit_code = main()
        sys.exit(exit_code)
    except KeyboardInterrupt:
        print(f"\n\n{Colors.WARNING}⚠️  Diagnostic interrompu par l'utilisateur{Colors.ENDC}")
        sys.exit(1)
    except Exception as e:
        print(f"\n\n{Colors.FAIL}❌ Erreur critique: {e}{Colors.ENDC}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
