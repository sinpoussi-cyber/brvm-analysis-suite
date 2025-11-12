# ==============================================================================
# API KEY MANAGER V11.0 - CLAUDE API (1 Clé)
# ==============================================================================

import os
import time
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s')


class APIKeyManager:
    """Gestionnaire de clé API Claude (1 seule clé)"""
    
    # État partagé entre toutes les instances
    _shared_state = {
        'api_key': None,
        'last_request_time': None,
        'requests_this_minute': 0,
        'minute_start_time': None,
        'usage_by_module': {}
    }
    
    def __init__(self, module_name='default'):
        self.__dict__ = self._shared_state
        self.module_name = module_name
        
        if not self.api_key:
            self._load_key()
            if self.api_key:
                logging.info(f"✅ [{module_name}] Clé Claude API chargée")
            else:
                logging.warning(f"⚠️  [{module_name}] Aucune clé Claude trouvée")
    
    def _load_key(self):
        """Charge la clé Claude depuis les variables d'environnement"""
        self.api_key = os.environ.get('CLAUDE_API_KEY')
    
    def get_api_key(self):
        """Retourne la clé API"""
        return self.api_key
    
    def handle_rate_limit(self):
        """Gestion du rate limiting Claude (50 req/min)"""
        now = datetime.now()
        
        if self.minute_start_time is None:
            self.minute_start_time = now
            self.requests_this_minute = 0
        
        # Reset compteur après 1 minute
        if (now - self.minute_start_time).total_seconds() >= 60:
            self.minute_start_time = now
            self.requests_this_minute = 0
        
        # Claude: 50 req/min
        if self.requests_this_minute >= 50:
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
        has_key = 1 if self.api_key else 0
        return {
            'total': has_key,
            'available': has_key,
            'used_by_module': self.usage_by_module.get(self.module_name, 0)
        }
