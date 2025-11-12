# ==============================================================================
# API KEY MANAGER V13.1 - GEMINI 2.0 FLASH (GESTION RATE LIMIT CORRIGÉE)
# ==============================================================================

import os
import time
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s')


class APIKeyManager:
    """Gestionnaire de clés API Gemini (2 clés avec rotation intelligente)"""
    
    # État partagé entre toutes les instances
    _shared_state = {
        'api_keys': [],
        'current_key_index': 0,
        'last_request_time': {},  # Par clé
        'requests_count': {},  # Compteur par clé
        'usage_by_module': {}
    }
    
    def __init__(self, module_name='default'):
        self.__dict__ = self._shared_state
        self.module_name = module_name
        
        if not self.api_keys:
            self._load_keys()
            if self.api_keys:
                logging.info(f"✅ [{module_name}] {len(self.api_keys)} clé(s) Gemini chargée(s)")
                # Initialiser les compteurs pour chaque clé
                for i in range(len(self.api_keys)):
                    self.last_request_time[i] = None
                    self.requests_count[i] = 0
            else:
                logging.warning(f"⚠️  [{module_name}] Aucune clé Gemini trouvée")
    
    def _load_keys(self):
        """Charge les clés Gemini depuis les variables d'environnement"""
        for i in range(1, 3):  # 2 clés
            key = os.environ.get(f'GEMINI_API_KEY_{i}')
            if key:
                self.api_keys.append(key)
    
    def get_api_key(self):
        """Retourne la clé API courante"""
        if not self.api_keys:
            return None
        return self.api_keys[self.current_key_index]
    
    def rotate_to_next_key(self):
        """Passe à la clé suivante (rotation)"""
        if len(self.api_keys) <= 1:
            return False
        
        old_index = self.current_key_index
        self.current_key_index = (self.current_key_index + 1) % len(self.api_keys)
        
        logging.info(f"🔄 [{self.module_name}] Rotation clé #{old_index + 1} → clé #{self.current_key_index + 1}")
        return True
    
    def handle_rate_limit(self):
        """
        Gestion intelligente du rate limiting Gemini
        - Limite : 15 requêtes par minute par clé (conservateur)
        - Rotation automatique entre les clés
        """
        now = datetime.now()
        current_key = self.current_key_index
        
        # Initialiser si première utilisation de cette clé
        if self.last_request_time.get(current_key) is None:
            self.last_request_time[current_key] = now
            self.requests_count[current_key] = 0
        
        last_request = self.last_request_time[current_key]
        time_since_last = (now - last_request).total_seconds()
        
        # Reset compteur si plus d'une minute s'est écoulée
        if time_since_last >= 60:
            self.requests_count[current_key] = 0
            self.last_request_time[current_key] = now
        
        # Si on a atteint la limite pour cette clé, rotation
        if self.requests_count[current_key] >= 15:
            # Attendre le reste de la minute si c'est la dernière clé
            if len(self.api_keys) == 1:
                sleep_time = 60 - time_since_last
                if sleep_time > 0:
                    logging.warning(f"⏸️  [{self.module_name}] Pause rate limit: {sleep_time:.1f}s")
                    time.sleep(sleep_time)
                    self.requests_count[current_key] = 0
                    self.last_request_time[current_key] = datetime.now()
            else:
                # Rotation vers la clé suivante
                self.rotate_to_next_key()
                current_key = self.current_key_index
                
                # Vérifier si la nouvelle clé est aussi limitée
                if self.requests_count.get(current_key, 0) >= 15:
                    last_req_new_key = self.last_request_time.get(current_key, now)
                    time_since_new_key = (now - last_req_new_key).total_seconds()
                    
                    if time_since_new_key < 60:
                        # Attendre que la nouvelle clé soit disponible
                        sleep_time = 60 - time_since_new_key
                        logging.warning(f"⏸️  [{self.module_name}] Toutes les clés limitées, pause: {sleep_time:.1f}s")
                        time.sleep(sleep_time)
                        self.requests_count[current_key] = 0
                        self.last_request_time[current_key] = datetime.now()
                    else:
                        # Reset si plus d'une minute
                        self.requests_count[current_key] = 0
                        self.last_request_time[current_key] = now
        
        # Petite pause entre chaque requête (4 secondes = 15 req/min max)
        if time_since_last < 4:
            sleep_time = 4 - time_since_last
            time.sleep(sleep_time)
        
        # Incrémenter le compteur
        self.requests_count[current_key] += 1
        self.last_request_time[current_key] = datetime.now()
        
        if self.module_name not in self.usage_by_module:
            self.usage_by_module[self.module_name] = 0
        self.usage_by_module[self.module_name] += 1
    
    def get_statistics(self):
        """Statistiques d'utilisation"""
        return {
            'total': len(self.api_keys),
            'available': len(self.api_keys),
            'current_key': self.current_key_index + 1,
            'used_by_module': self.usage_by_module.get(self.module_name, 0)
        }
