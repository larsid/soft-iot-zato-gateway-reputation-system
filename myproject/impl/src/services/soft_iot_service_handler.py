# -*- coding: utf-8 -*-

import os
import time
from zato.server.service import Service

class CalculateReputationTask(Service):
    """
    Equivalente ao CalculateNodeReputationTask.java.
    Percorre a lista de nós conhecidos e dispara o cálculo de reputação.
    """
    name = 'soft-iot.reputation.task.calculate'

    def handle(self):
        self.logger.info("Iniciando Task de Cálculo de Reputação Global.")

        # 1. Obter lista de dispositivos/nós conhecidos
        # No Java, isso vem do storage ou discovery. Aqui usamos o mapping local.
        mapping_res = self.invoke('soft-iot.mapping.list-devices')
        devices = mapping_res.get('devices', [])

        if not devices:
            self.logger.warning("Nenhum dispositivo encontrado para calcular reputação.")
            return

        # 2. Iterar sobre os nós para atualizar a reputação na Tangle/Cache
        for device in devices:
            node_id = device.get('id')
            if not node_id:
                continue

            self.logger.info(f"Processando reputação para o nó: {node_id}")

            # Invoca o Orchestrator
            reputation_res = self.invoke('soft-iot.reputation.orchestrator', {'node_id': node_id})

            if reputation_res.get('status') == 'success':
                new_reputation = reputation_res.get('reputation')
                self.logger.info(f"Nova reputação para {node_id}: {new_reputation}")
                
            else:
                self.logger.error(f"Falha ao calcular reputação para {node_id}: {reputation_res.get('message')}")

        self.logger.info("Task de Cálculo de Reputação finalizada.")