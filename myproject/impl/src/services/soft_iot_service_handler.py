# -*- coding: utf-8 -*-

import os
import json
from zato.server.service import Service


STATE_FILE = '/tmp/gateway_reputation.json'


class CalculateNodeReputationTask(Service):
    """
    Calcula a reputação do próprio Gateway baseado nas avaliações da Tangle.
    """
    name = 'soft-iot.reputation.task.calculate'

    def handle(self):
        self.logger.info("Executando CalculateNodeReputationTask...")

        identity_info = self.invoke('soft-iot.id.manager')
        my_gateway_id = identity_info.get('gateway_id')

        if not my_gateway_id:
            # Retorna o valor sob demanda (a escrita desnecessária de consenso na Tangle foi removida)
            self.response.payload = {"status": "failure","justification": "no id"}
            return

        # Calcula a própria reputação
        rep_result = self.invoke('soft-iot.reputation.orchestrator', {'node_id': my_gateway_id})

        if rep_result.get('status') == 'success':
            my_current_reputation = rep_result.get('reputation')
            
            self._save_reputation_to_file(my_current_reputation)
            
            self.logger.info(f"SUCESSO: Minha reputação salva no arquivo local: {my_current_reputation}")

            self.response.payload = {"status": "success"}

    def _save_reputation_to_file(self, reputation_value):
        """ Função auxiliar para gravar o valor no arquivo JSON """
        state_data = {}
        
        if os.path.exists(STATE_FILE):
            try:
                with open(STATE_FILE, 'r') as f:
                    state_data = json.load(f)
            except Exception:
                pass 
                
        # Atualiza a chave com o novo valor calculado
        state_data['my_global_reputation'] = reputation_value
        
        # Grava o dicionário atualizado de volta no arquivo
        try:
            with open(STATE_FILE, 'w') as f:
                json.dump(state_data, f, indent=4)
        except Exception as e:
            self.logger.error(f"Falha ao salvar no arquivo de estado: {e}")

    def _get_my_reputation(self):
        """ Função auxiliar para ler a reputação do arquivo """
        if os.path.exists(STATE_FILE):
            try:
                with open(STATE_FILE, 'r') as f:
                    state_data = json.load(f)
                    return float(state_data.get('my_global_reputation', 0.5))
            except Exception:
                pass
        
        # Se o arquivo não existir ou falhar, retorna o valor inicial/neutro
        return 0.5