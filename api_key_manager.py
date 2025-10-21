# ==============================================================================
# MODULE: API KEY MANAGER - GESTIONNAIRE CENTRALISÉ DES CLÉS API GEMINI
# ==============================================================================
# Ce module gère intelligemment l'utilisation des clés API entre les différents
# modules (fundamental_analyzer et report_generator)
# ==============================================================================

import os
import json
import time
import logging
from datetime import datetime
from pathlib import Path

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s')

class APIKeyManager:
    """Gestionnaire centralisé des clés API Gemini avec état partagé"""
    
    STATE_FILE = "/tmp/api_keys_state.json"
    
    def __init__(self, module_name):
        """
        Initialize le gestionnaire pour un module spécifique
        
        Args:
            module_name: Nom du module ('fundamental_analyzer' ou 'report_generator')
        """
        self.module_name = module_name
        self.all_keys = []
        self.available_keys = []
        self.exhausted_keys = []
        self.current_key_index = 0
        self.request_timestamps = []
        self.requests_per_minute_limit = 15
        
        self._load_all_keys()
        self._load_state()
        
    def _load_all_keys(self):
        """Charge toutes les clés API disponibles (1 à 100)"""
        for i in range(1, 101):
            key = os.environ.get(f'GOOGLE_API_KEY_{i}')
            if key:
                self.all_keys.append({
                    'number': i,
                    'key': key.strip(),
                    'used_by': None,
                    'exhausted': False
                })
        
        logging.info(f"✅ [{self.module_name}] {len(self.all_keys)} clé(s) API trouvée(s)")
    
    def _load_state(self):
        """Charge l'état des clés depuis le fichier partagé"""
        try:
            if os.path.exists(self.STATE_FILE):
                with open(self.STATE_FILE, 'r') as f:
                    state = json.load(f)
                
                # Marquer les clés épuisées
                for key_info in self.all_keys:
                    key_num = key_info['number']
                    if str(key_num) in state.get('exhausted_keys', []):
                        key_info['exhausted'] = True
                        self.exhausted_keys.append(key_num)
                    elif str(key_num) in state.get('used_by', {}):
                        key_info['used_by'] = state['used_by'][str(key_num)]
                
                logging.info(f"📂 [{self.module_name}] État chargé: {len(self.exhausted_keys)} clé(s) épuisée(s)")
            else:
                logging.info(f"📂 [{self.module_name}] Nouvel état (fichier n'existe pas)")
        except Exception as e:
            logging.warning(f"⚠️  [{self.module_name}] Erreur chargement état: {e}")
    
    def _save_state(self):
        """Sauvegarde l'état des clés dans le fichier partagé"""
        try:
            state = {
                'timestamp': datetime.now().isoformat(),
                'exhausted_keys': [str(k) for k in self.exhausted_keys],
                'used_by': {}
            }
            
            for key_info in self.all_keys:
                if key_info['used_by']:
                    state['used_by'][str(key_info['number'])] = key_info['used_by']
            
            with open(self.STATE_FILE, 'w') as f:
                json.dump(state, f, indent=2)
                
        except Exception as e:
            logging.warning(f"⚠️  [{self.module_name}] Erreur sauvegarde état: {e}")
    
    def get_available_keys(self):
        """Retourne les clés disponibles pour ce module"""
        available = []
        
        for key_info in self.all_keys:
            # Ignorer les clés épuisées
            if key_info['exhausted']:
                continue
            
            # Ignorer les clés utilisées par un autre module
            if key_info['used_by'] and key_info['used_by'] != self.module_name:
                continue
            
            available.append(key_info)
        
        self.available_keys = available
        logging.info(f"✅ [{self.module_name}] {len(available)} clé(s) disponible(s)")
        return available
    
    def get_next_key(self):
        """Retourne la prochaine clé API disponible"""
        available = self.get_available_keys()
        
        if not available:
            logging.error(f"❌ [{self.module_name}] Aucune clé disponible")
            return None
        
        if self.current_key_index >= len(available):
            self.current_key_index = 0
        
        key_info = available[self.current_key_index]
        
        # Marquer comme utilisée par ce module
        if not key_info['used_by']:
            key_info['used_by'] = self.module_name
            self._save_state()
        
        return key_info
    
    def mark_key_exhausted(self, key_number):
        """Marque une clé comme épuisée"""
        for key_info in self.all_keys:
            if key_info['number'] == key_number:
                key_info['exhausted'] = True
                if key_number not in self.exhausted_keys:
                    self.exhausted_keys.append(key_number)
                break
        
        self._save_state()
        logging.warning(f"⚠️  [{self.module_name}] Clé #{key_number} marquée comme épuisée")
    
    def move_to_next_key(self):
        """Passe à la clé suivante"""
        self.current_key_index += 1
    
    def handle_rate_limit(self):
        """Gère le rate limiting (15 requêtes/minute)"""
        now = time.time()
        self.request_timestamps = [ts for ts in self.request_timestamps if now - ts < 60]
        
        if len(self.request_timestamps) >= self.requests_per_minute_limit:
            sleep_time = 60 - (now - self.request_timestamps[0]) if self.request_timestamps else 60
            logging.warning(f"⏸️  [{self.module_name}] Pause rate limit: {sleep_time + 1:.1f}s")
            time.sleep(sleep_time + 1)
            self.request_timestamps = []
        
        self.request_timestamps.append(time.time())
    
    def get_statistics(self):
        """Retourne les statistiques d'utilisation"""
        total = len(self.all_keys)
        exhausted = len(self.exhausted_keys)
        available = len(self.get_available_keys())
        
        return {
            'total': total,
            'exhausted': exhausted,
            'available': available,
            'used_by_module': sum(1 for k in self.all_keys if k['used_by'] == self.module_name)
        }
    
    def reset_state(self):
        """Réinitialise l'état (pour debug)"""
        if os.path.exists(self.STATE_FILE):
            os.remove(self.STATE_FILE)
            logging.info(f"🔄 [{self.module_name}] État réinitialisé")

# ==============================================================================
# FONCTIONS UTILITAIRES
# ==============================================================================

def cleanup_state_file():
    """Nettoie le fichier d'état (à appeler au début du workflow)"""
    state_file = "/tmp/api_keys_state.json"
    if os.path.exists(state_file):
        os.remove(state_file)
        logging.info("🧹 Fichier d'état nettoyé")

def get_global_statistics():
    """Retourne les statistiques globales"""
    try:
        if not os.path.exists("/tmp/api_keys_state.json"):
            return None
        
        with open("/tmp/api_keys_state.json", 'r') as f:
            state = json.load(f)
        
        return {
            'timestamp': state.get('timestamp'),
            'exhausted_keys': len(state.get('exhausted_keys', [])),
            'modules': list(set(state.get('used_by', {}).values()))
        }
    except:
        return None
