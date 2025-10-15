#!/usr/bin/env python3
# ==============================================================================
# SCRIPT DE DIAGNOSTIC AUTOMATIQUE - API GEMINI
# ==============================================================================
# Ce script diagnostique automatiquement les problèmes avec l'API Gemini
# et propose des solutions concrètes.
# ==============================================================================

import os
import sys
import requests
import json
import re
from datetime import datetime

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
        """Charge toutes les clés API disponibles"""
        print_info("Chargement des clés API...")
        for i in range(1, 34):
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
        
        # Configuration correcte v7.4
        url = "https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent"
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
                key_
