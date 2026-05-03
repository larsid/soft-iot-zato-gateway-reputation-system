# -*- coding: utf-8 -*-

import os
import time
from zato.server.service import Service

class CalculateNodeReputationTask(Service):
    """
    Equivalente exato ao CalculateNodeReputationTask.java.
    Calcula a reputação do próprio Gateway baseado nas avaliações da Tangle.
    """
    name = 'soft-iot.reputation.task.calculate'

    def handle(self):
        self.logger.info("Executando CalculateNodeReputationTask...")

        # 1. Obtém o ID do PRÓPRIO Gateway (equivalente ao this.getId() do Java)
        identity_info = self.invoke('soft-iot.id.manager')
        my_gateway_id = identity_info.get('gateway_id')

        if not my_gateway_id:
            self.logger.error("Erro: ID do Gateway não configurado.")
            return

        self.logger.info(f"Calculando a própria reputação global para o ID: {my_gateway_id}")

        # 2. Chama o Orchestrator passando o próprio ID como alvo
        rep_result = self.invoke('soft-iot.reputation.orchestrator', {'node_id': my_gateway_id})

        if rep_result.get('status') == 'success':
            # 3. Atualiza o estado interno (equivalente ao this.setReputation(rep) do Java)
            my_current_reputation = rep_result.get('reputation')
            
            self.logger.info(f"SUCESSO: Minha reputação global atualizada para {my_current_reputation}")
            
            # Aqui você deve salvar esse valor internamente para uso do Gateway
            # Pode ser no Redis nativo do Zato ou gravar no SQLite do Gateway
            # Exemplo de uso de cache simples do Zato:
            self.cache.set('my_global_reputation', my_current_reputation)
            
        else:
            self.logger.error(f"Falha ao calcular a reputação: {rep_result.get('message')}")