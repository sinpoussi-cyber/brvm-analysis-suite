# ==============================================================================
# API KEY MANAGER V13.0 - GEMINI 2.0 FLASH (2 Clés)
# ==============================================================================

import os
import time
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s')


class APIKeyManager:
    """Gestionnaire de clés API Gemini (2 clés pour redondance)"""
    
    # État partagé entre toutes les instances
    _shared_state = {
        'api_keys': [],
        'current_key_index': 0,
        'last_request_time': None,
        'requests_this_minute': 0,
        'minute_start_time': None,
        'usage_by_module': {},
        'failed_keys': set()
    }
    
    def __init__(self, module_name='default'):
        self.__dict__ = self._shared_state
        self.module_name = module_name
        
        if not self.api_keys:
            self._load_keys()
            if self.api_keys:
                logging.info(f"✅ [{module_name}] {len(self.api_keys)} clé(s) Gemini chargée(s)")
            else:
                logging.warning(f"⚠️  [{module_name}] Aucune clé Gemini trouvée")
    
    def _load_keys(self):
        """Charge les clés Gemini depuis les variables d'environnement"""
        for i in range(1, 3):  # 2 clés
            key = os.environ.get(f'GEMINI_API_KEY_{i}')
            if key:
                self.api_keys.append(key)
    
    def get_api_key(self):
        """Retourne la clé API courante (avec rotation en cas d'échec)"""
        if not self.api_keys:
            return None
        
        # Utiliser la clé courante
        return self.api_keys[self.current_key_index]
    
    def mark_key_failed(self):
        """Marque la clé courante comme échouée et passe à la suivante"""
        failed_key = self.api_keys[self.current_key_index]
        self.failed_keys.add(failed_key)
        
        logging.warning(f"⚠️  [{self.module_name}] Clé #{self.current_key_index + 1} marquée comme échouée")
        
        # Passer à la clé suivante
        self.current_key_index = (self.current_key_index + 1) % len(self.api_keys)
        
        # Si toutes les clés ont échoué
        if len(self.failed_keys) >= len(self.api_keys):
            logging.error(f"❌ [{self.module_name}] Toutes les clés ont échoué")
            return False
        
        return True
    
    def handle_rate_limit(self):
        """Gestion du rate limiting Gemini (15 req/min par clé)"""
        now = datetime.now()
        
        if self.minute_start_time is None:
            self.minute_start_time = now
            self.requests_this_minute = 0
        
        # Reset compteur après 1 minute
        if (now - self.minute_start_time).total_seconds() >= 60:
            self.minute_start_time = now
            self.requests_this_minute = 0
        
        # Gemini: 15 req/min par clé (conservateur)
        if self.requests_this_minute >= 15:
            sleep_time = 60 - (now - self.minute_start_time).total_seconds()
            if sleep_time > 0:
                logging.warning(f"⏸️  [{self.module_name}] Pause rate limit: {sleep_time:.1f}s")
                time.sleep(sleep_time)
                self.minute_start_time = datetime.now()
                self.requests_this_minute = 0
                
                # Rotation vers clé suivante après pause
                self.current_key_index = (self.current_key_index + 1) % len(self.api_keys)
        
        self.requests_this_minute += 1
        self.last_request_time = now
        
        if self.module_name not in self.usage_by_module:
            self.usage_by_module[self.module_name] = 0
        self.usage_by_module[self.module_name] += 1
    
    def get_statistics(self):
        """Statistiques d'utilisation"""
        available = len(self.api_keys) - len(self.failed_keys)
        return {
            'total': len(self.api_keys),
            'available': available,
            'failed': len(self.failed_keys),
            'used_by_module': self.usage_by_module.get(self.module_name, 0)
        }
