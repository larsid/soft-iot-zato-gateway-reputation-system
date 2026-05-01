# -*- coding: utf-8 -*-

import os
import threading
import logging
import random

from zato.server.service import Service

from soft_iot_id_manager import IDManager

logger = logging.getLogger('zato.node.type')

class NodeTypeManager:
    """
    Controlador Singleton para gerenciar o tipo e a conduta do nó.
    Substitui a lógica central do NodeType.java e as configurações do br.uefs.larsid.soft_iot.node_type.cfg.
    """

    _instance = None
    _lock = threading.Lock()  # Evita múltiplas instâncias com acessos simultâneos

    def __new__(cls):
        # Implementação do Singleton. Cria uma nova instância, se não houver.
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super(NodeTypeManager, cls).__new__(cls)
                    
                    # 1. Carrega as configurações das variáveis de ambiente
                    # Tipos: 1 - Honesto, 2 - Malicioso, 3 - Egoista, 4 - Perturbador
                    cls._instance._node_type = int(os.environ.get('Zato_NODE_TYPE', 1))

                    # Taxa de honestidade
                    cls._instance._honesty_rate = float(os.environ.get('Zato_HONESTY_RATE', 50.0))
                    
                    # Valor inicial da conduta
                    cls._instance._current_conduct = "HONEST"
                    
                    logger.info(f"NodeTypeManager iniciado - Tipo: {cls._instance._node_type} - Taxa de honestidade: {cls._instance._honesty_rate}%")
                    
                    # Inicializa a conduta baseada no tipo
                    cls._instance.define_conduct()

        return cls._instance

    def define_conduct(self, current_reputation=None):
        """
        Define a conduta baseada no tipo e na reputação (para o Perturbador).
        Reflete a lógica de Disturbing.java e Malicious.java.
        """
        # 1 - Honesto 
        if self._node_type == 1:
            self._current_conduct = "HONEST"
            
        # 2 - Malicioso
        elif self._node_type == 2:
            # Mantém o sorteio probabilístico do Malicious.java
            random_number = random.uniform(0, 100)
            if random_number > self._honesty_rate:
                self._current_conduct = "MALICIOUS"
            else:
                self._current_conduct = "HONEST"
                
        # 3 - Egoísta
        elif self._node_type == 3:
            self._current_conduct = "SELFISH" 
            
        # 4 - Perturbador (Lógica fiel ao Java)
        elif self._node_type == 4:
            # No Java, o perturbador constrói reputação para depois atacar
            threshold = float(os.environ.get('Zato_PERTURBATION_THRESHOLD', 0.5))
            
            # Se a reputação atual for maior que o limite, ele inicia o ataque
            if current_reputation is not None and current_reputation >= threshold:
                self._current_conduct = "MALICIOUS"
            else:
                self._current_conduct = "HONEST"


    @property
    def current_conduct(self):
        """Retorna a conduta atual."""
        return self._current_conduct

    @property
    def node_type(self):
        """Retorna o tipo base do nó."""
        return self._node_type

    @property
    def honesty_rate(self):
        """Retorna a taxa de honestidade configurada."""
        return self._honesty_rate
    

class NodeEvaluationService(Service):
    """
    Serviço que executa a conduta do nó ao avaliar um serviço.
    Substitui o método evaluateServiceProvider das classes de conduta Java.
    """
    name = 'soft-iot.node.evaluation'

    def handle(self):

        nt_manager = NodeTypeManager()
        id_m = IDManager()

        # Dados da avaliação 
        data = self.request.payload
        provider_id = data.get('provider_id')
        original_value = data.get('value') # 1 para bom, 0 para ruim
        credibility = data.get('credibility', 1.0)


        current_rep = None
        if nt_manager.node_type == 4:
            # Busca a PRÓPRIA reputação na Tangle via DLT Client
            rep_res = self.invoke('soft-iot.reputation.get_value', {'node_id': id_m.id})
            current_rep = rep_res.get('reputation_value', 0.0)


        # Consultando condulta
        nt_manager.define_conduct(current_reputation=current_rep)
        conduct = nt_manager.current_conduct

        if conduct == "SELFISH":
            # Egoísta não avalia 
            self.logger.info(f"Nó Egoísta ignorando avaliação para {provider_id}")
            return {"status": "ignored"}

        final_value = original_value
        
        if conduct == "MALICIOUS":
            # Malicioso avalia como ruim
            self.logger.info(f"Ataque Malicioso: Alterando avaliação de {original_value} para 0")
            final_value = 0

        # Preparando transação para a Tangle
        evaluation_transaction = {
            "type": "REP_EVALUATION",
            "origin": id_m.id,   
            "group": id_m.group,   
            "providerId": provider_id,
            "value": final_value,
            "credibility": credibility
        }

        # Envia para a Tangle 
        res = self.invoke('soft-iot.dlt.client.api.write', {
            "index": provider_id,
            "data": evaluation_transaction
        })

        return {"status": "success", "conduct_applied": conduct, "tangle_response": res}
    
