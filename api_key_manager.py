# ==============================================================================
# API KEY MANAGER V16.0 - GEMINI 1.5 FLASH (SUPPORT 11 CLÉS)
# ==============================================================================

import os
import time
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s')


class APIKeyManager:
    """Gestionnaire de clés API Gemini avec rotation intelligente (jusqu'à 15 clés)"""
    
    # État partagé entre toutes les instances
    _shared_state = {
        'api_keys': [],
        'current_key_index': 0,
        'key_request_counts': {},  # Compteur par clé
        'key_reset_times': {},     # Temps de reset par clé
        'usage_by_module': {},
        'failed_attempts_per_key': {}  # Compteur d'échecs par clé
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
                    self.failed_attempts_per_key[key] = 0
            else:
                logging.warning(f"⚠️  [{module_name}] Aucune clé Gemini trouvée")
    
    def _load_keys(self):
        """Charge jusqu'à 15 clés Gemini depuis les variables d'environnement"""
        for i in range(1, 16):  # Support jusqu'à 15 clés
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
            self.failed_attempts_per_key[current_key] = 0
            logging.info(f"🔄 [{self.module_name}] Clé #{self.current_key_index + 1} réinitialisée")
        
        # Si la clé courante a atteint la limite, passer à la suivante
        if self.key_request_counts[current_key] >= 10:  # Limite conservatrice : 10 req/min
            # Essayer les autres clés
            for _ in range(len(self.api_keys)):
                self.current_key_index = (self.current_key_index + 1) % len(self.api_keys)
                next_key = self.api_keys[self.current_key_index]
                
                # Vérifier si cette clé est disponible
                if (now - self.key_reset_times[next_key]).total_seconds() >= 60:
                    self.key_request_counts[next_key] = 0
                    self.key_reset_times[next_key] = now
                    self.failed_attempts_per_key[next_key] = 0
                
                if self.key_request_counts[next_key] < 10:
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
                    self.failed_attempts_per_key[key] = 0
        
        return current_key
    
    def record_request(self):
        """Enregistre une requête pour la clé courante"""
        current_key = self.api_keys[self.current_key_index]
        self.key_request_counts[current_key] += 1
        
        if self.module_name not in self.usage_by_module:
            self.usage_by_module[self.module_name] = 0
        self.usage_by_module[self.module_name] += 1
    
    def record_failure(self):
        """Enregistre un échec pour la clé courante"""
        current_key = self.api_keys[self.current_key_index]
        self.failed_attempts_per_key[current_key] += 1
        
        # Si trop d'échecs consécutifs, forcer la rotation
        if self.failed_attempts_per_key[current_key] >= 3:
            logging.warning(f"⚠️  [{self.module_name}] Clé #{self.current_key_index + 1} : 3 échecs consécutifs, rotation forcée")
            self.key_request_counts[current_key] = 10  # Forcer au max
            return False
        return True
    
    def handle_rate_limit_response(self):
        """Gère une réponse 429 (rate limit) - SANS récursion infinie"""
        current_key = self.api_keys[self.current_key_index]
        logging.warning(f"⚠️  [{self.module_name}] Rate limit sur clé #{self.current_key_index + 1}")
        
        # Enregistrer l'échec
        self.record_failure()
        
        # Forcer le compteur au max pour cette clé
        self.key_request_counts[current_key] = 10
        
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
                self.failed_attempts_per_key[next_key] = 0
            
            # Si cette clé a moins de 2 échecs, l'utiliser
            if self.failed_attempts_per_key[next_key] < 2:
                logging.info(f"✅ [{self.module_name}] Basculé sur clé #{self.current_key_index + 1}")
                return True
        
        # Si toutes les clés ont échoué, retourner False (pas de récursion)
        self.current_key_index = original_index
        logging.error(f"❌ [{self.module_name}] TOUTES LES CLÉS ONT ÉCHOUÉ - Utilisation du fallback")
        return False
    
    def get_statistics(self):
        """Statistiques d'utilisation"""
        available = sum(1 for k in self.api_keys if self.failed_attempts_per_key[k] < 2)
        return {
            'total': len(self.api_keys),
            'available': available,
            'used_by_module': self.usage_by_module.get(self.module_name, 0)
        }
