# ==============================================================================
# API KEY MANAGER V10.0 - FINAL (2 Clés AI Studio)
# ==============================================================================

import os
import time
import logging
from datetime import datetime, timedelta

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s')


class APIKeyManager:
    """Gestionnaire de clés API Gemini avec support 2 clés"""
    
    # État partagé entre toutes les instances
    _shared_state = {
        'keys': {},
        'current_index': 0,
        'last_request_time': None,
        'requests_this_minute': 0,
        'minute_start_time': None,
        'exhausted_keys': set(),
        'usage_by_module': {}
    }
    
    def __init__(self, module_name='default'):
        self.__dict__ = self._shared_state
        self.module_name = module_name
        
        if not self.keys:
            self._load_keys()
            logging.info(f"✅ [{module_name}] {len(self.keys)} clé(s) API trouvée(s)")
            
            if self.keys:
                state_file = '/tmp/api_key_state.txt'
                if os.path.exists(state_file):
                    logging.info(f"📂 [{module_name}] État existant chargé")
                else:
                    logging.info(f"📂 [{module_name}] Nouvel état (fichier n'existe pas)")
    
    def _load_keys(self):
        """Charge les 2 clés depuis les variables d'environnement"""
        for i in range(1, 3):  # 2 clés seulement
            key = os.environ.get(f'GOOGLE_API_KEY_{i}')
            if key:
                self.keys[i] = {
                    'key': key,
                    'number': i,
                    'requests_count': 0,
                    'last_used': None
                }
        
        if not self.keys:
            logging.warning("⚠️  Aucune clé API trouvée")
    
    def get_available_keys(self):
        """Retourne les clés non épuisées"""
        return [k for num, k in self.keys.items() if num not in self.exhausted_keys]
    
    def get_next_key(self):
        """Obtient la prochaine clé disponible"""
        available = self.get_available_keys()
        
        if not available:
            logging.warning(f"⚠️  [{self.module_name}] Toutes les clés épuisées")
            return None
        
        key_info = available[self.current_index % len(available)]
        logging.info(f"✅ [{self.module_name}] {len(available)} clé(s) disponible(s)")
        
        return key_info
    
    def move_to_next_key(self):
        """Passe à la clé suivante"""
        self.current_index += 1
    
    def mark_key_exhausted(self, key_number):
        """Marque une clé comme épuisée"""
        self.exhausted_keys.add(key_number)
        logging.warning(f"🚫 [{self.module_name}] Clé #{key_number} épuisée")
    
    def handle_rate_limit(self):
        """Gestion du rate limiting (15 req/min par clé)"""
        now = datetime.now()
        
        if self.minute_start_time is None:
            self.minute_start_time = now
            self.requests_this_minute = 0
        
        if (now - self.minute_start_time).total_seconds() >= 60:
            self.minute_start_time = now
            self.requests_this_minute = 0
        
        # 2 clés = 30 req/min max
        if self.requests_this_minute >= 30:
            sleep_time = 60 - (now - self.minute_start_time).total_seconds()
            if sleep_time > 0:
                logging.warning(f"⏸️  [{self.module_name}] Pause rate limit: {sleep_time:.1f}s")
                time.sleep(sleep_time)
                self.minute_start_time = datetime.now()
                self.requests_this_minute = 0
        
        self.requests_this_minute += 1
        self.last_request_time = now
        
        if self.module_name not in self.usage_by_module:
            self.usage_by_module[self.module_name] = 0
        self.usage_by_module[self.module_name] += 1
    
    def get_statistics(self):
        """Statistiques d'utilisation"""
        available = self.get_available_keys()
        return {
            'total': len(self.keys),
            'available': len(available),
            'exhausted': len(self.exhausted_keys),
            'used_by_module': self.usage_by_module.get(self.module_name, 0)
        }
