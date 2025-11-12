# ==============================================================================
# API KEY MANAGER V14.0 - GEMINI 2.0 FLASH (ROTATION CORRIGÉE)
# ==============================================================================

import os
import time
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s')


class APIKeyManager:
    """Gestionnaire de clés API Gemini avec vraie rotation"""
    
    # État partagé entre toutes les instances
    _shared_state = {
        'api_keys': [],
        'current_key_index': 0,
        'key_request_counts': {},  # Compteur par clé
        'key_reset_times': {},     # Temps de reset par clé
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
                for key in self.api_keys:
                    self.key_request_counts[key] = 0
                    self.key_reset_times[key] = datetime.now()
            else:
                logging.warning(f"⚠️  [{module_name}] Aucune clé Gemini trouvée")
    
    def _load_keys(self):
        """Charge les clés Gemini depuis les variables d'environnement"""
        for i in range(1, 3):  # 2 clés
            key = os.environ.get(f'GEMINI_API_KEY_{i}')
            if key:
                self.api_keys.append(key)
    
    def get_api_key(self):
        """Retourne une clé API disponible (avec rotation automatique)"""
        if not self.api_keys:
            return None
        
        # Vérifier si la clé courante a besoin d'un reset
        current_key = self.api_keys[self.current_key_index]
        now = datetime.now()
        
        # Reset du compteur après 60 secondes
        if (now - self.key_reset_times[current_key]).total_seconds() >= 60:
            self.key_request_counts[current_key] = 0
            self.key_reset_times[current_key] = now
            logging.info(f"🔄 [{self.module_name}] Clé #{self.current_key_index + 1} réinitialisée")
        
        # Si la clé courante a atteint la limite, passer à la suivante
        if self.key_request_counts[current_key] >= 15:
            # Essayer les autres clés
            for _ in range(len(self.api_keys)):
                self.current_key_index = (self.current_key_index + 1) % len(self.api_keys)
                next_key = self.api_keys[self.current_key_index]
                
                # Vérifier si cette clé est disponible
                if (now - self.key_reset_times[next_key]).total_seconds() >= 60:
                    self.key_request_counts[next_key] = 0
                    self.key_reset_times[next_key] = now
                
                if self.key_request_counts[next_key] < 15:
                    logging.info(f"🔄 [{self.module_name}] Rotation → Clé #{self.current_key_index + 1}")
                    return next_key
            
            # Si toutes les clés sont au max, attendre
            wait_time = 60 - min(
                (now - self.key_reset_times[k]).total_seconds() 
                for k in self.api_keys
            )
            if wait_time > 0:
                logging.warning(f"⏸️  [{self.module_name}] Toutes les clés en pause: {wait_time:.1f}s")
                time.sleep(wait_time + 1)
                # Reset toutes les clés
                for key in self.api_keys:
                    self.key_request_counts[key] = 0
                    self.key_reset_times[key] = datetime.now()
        
        return current_key
    
    def record_request(self):
        """Enregistre une requête pour la clé courante"""
        current_key = self.api_keys[self.current_key_index]
        self.key_request_counts[current_key] += 1
        
        if self.module_name not in self.usage_by_module:
            self.usage_by_module[self.module_name] = 0
        self.usage_by_module[self.module_name] += 1
        
        logging.debug(f"📊 Clé #{self.current_key_index + 1}: {self.key_request_counts[current_key]}/15 requêtes")
    
    def handle_rate_limit_response(self):
        """Gère une réponse 429 (rate limit)"""
        current_key = self.api_keys[self.current_key_index]
        logging.warning(f"⚠️  [{self.module_name}] Rate limit détecté sur clé #{self.current_key_index + 1}")
        
        # Forcer le compteur au max pour cette clé
        self.key_request_counts[current_key] = 15
        
        # Essayer de passer à une autre clé
        original_index = self.current_key_index
        for _ in range(len(self.api_keys) - 1):
            self.current_key_index = (self.current_key_index + 1) % len(self.api_keys)
            next_key = self.api_keys[self.current_key_index]
            
            # Vérifier si cette clé est disponible
            now = datetime.now()
            if (now - self.key_reset_times[next_key]).total_seconds() >= 60:
                self.key_request_counts[next_key] = 0
                self.key_reset_times[next_key] = now
            
            if self.key_request_counts[next_key] < 15:
                logging.info(f"✅ [{self.module_name}] Basculé sur clé #{self.current_key_index + 1}")
                return True
        
        # Si aucune clé disponible, attendre
        self.current_key_index = original_index
        logging.warning(f"⏸️  [{self.module_name}] Pause 60s (toutes les clés limitées)")
        time.sleep(60)
        
        # Reset toutes les clés
        for key in self.api_keys:
            self.key_request_counts[key] = 0
            self.key_reset_times[key] = datetime.now()
        
        return True
    
    def get_statistics(self):
        """Statistiques d'utilisation"""
        available = sum(1 for k in self.api_keys if self.key_request_counts[k] < 15)
        return {
            'total': len(self.api_keys),
            'available': available,
            'used_by_module': self.usage_by_module.get(self.module_name, 0)
        }
