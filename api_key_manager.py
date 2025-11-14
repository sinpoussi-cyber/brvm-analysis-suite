# ==============================================================================
# API KEY MANAGER V22.0 - CLAUDE API (SUPPORT 1 CLÉ)
# ==============================================================================

import os
import time
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s')


class APIKeyManager:
    """Gestionnaire de clé API Claude (une seule clé)"""
    
    def __init__(self, module_name='default'):
        self.module_name = module_name
        self.api_key = None
        self.request_count = 0
        self.reset_time = datetime.now()
        self.failed_attempts = 0
        
        self._load_key()
        
        if self.api_key:
            logging.info(f"✅ [{module_name}] Clé Claude chargée")
        else:
            logging.warning(f"⚠️  [{module_name}] Aucune clé Claude trouvée")
    
    def _load_key(self):
        """Charge la clé Claude depuis les variables d'environnement"""
        self.api_key = os.environ.get('CLAUDE_API_KEY')
    
    def get_api_key(self):
        """Retourne la clé API Claude"""
        if not self.api_key:
            return None
        
        now = datetime.now()
        
        # Reset du compteur après 60 secondes
        if (now - self.reset_time).total_seconds() >= 60:
            self.request_count = 0
            self.reset_time = now
            self.failed_attempts = 0
            logging.info(f"🔄 [{self.module_name}] Compteur réinitialisé")
        
        # Claude API : limite de 50 requêtes/minute (tier 1)
        if self.request_count >= 45:  # Limite conservatrice
            wait_time = 60 - (now - self.reset_time).total_seconds()
            if wait_time > 0:
                logging.warning(f"⏸️  [{self.module_name}] Pause rate limit: {wait_time:.1f}s")
                time.sleep(wait_time + 1)
                self.request_count = 0
                self.reset_time = datetime.now()
                self.failed_attempts = 0
        
        return self.api_key
    
    def record_request(self):
        """Enregistre une requête"""
        self.request_count += 1
    
    def record_failure(self):
        """Enregistre un échec"""
        self.failed_attempts += 1
        return self.failed_attempts < 3
    
    def handle_rate_limit_response(self):
        """Gère une réponse 429 (rate limit)"""
        logging.warning(f"⚠️  [{self.module_name}] Rate limit détecté")
        self.failed_attempts += 1
        
        # Attendre 60 secondes
        logging.info(f"⏸️  [{self.module_name}] Pause 60s pour rate limit")
        time.sleep(60)
        self.request_count = 0
        self.reset_time = datetime.now()
        
        return self.failed_attempts < 3
    
    def get_statistics(self):
        """Statistiques d'utilisation"""
        return {
            'total': 1,
            'available': 1 if self.failed_attempts < 3 else 0,
            'used_by_module': self.request_count
        }
