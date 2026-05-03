# -*- coding: utf-8 -*-

import os
import json
from zato.server.service import Service
from soft_iot_node_type import NodeTypeManager


class ServiceHandlerBaseService(Service):
    """
    Classe base de acesso ao arquivo de armazenamento da reputação local. 
    """

    STATE_FILE = '/tmp/gateway_reputation.json'

    def _save_reputation_to_file(self, reputation_value):
        """ Função auxiliar para gravar o valor no arquivo JSON """
        state_data = {}
        
        if os.path.exists(self.STATE_FILE):
            try:
                with open(self.STATE_FILE, 'r') as f:
                    state_data = json.load(f)
            except Exception:
                pass 
                
        # Atualiza a chave com o novo valor calculado
        state_data['my_global_reputation'] = reputation_value
        
        # Grava o dicionário atualizado de volta no arquivo
        try:
            with open(self.STATE_FILE, 'w') as f:
                json.dump(state_data, f, indent=4)
        except Exception as e:
            self.logger.error(f"Falha ao salvar no arquivo de estado: {e}")

    def _get_my_reputation(self):
        """ Função auxiliar para ler a reputação do arquivo """
        if os.path.exists(self.STATE_FILE):
            try:
                with open(self.STATE_FILE, 'r') as f:
                    state_data = json.load(f)
                    return float(state_data.get('my_global_reputation', 0.5))
            except Exception:
                pass
        
        # Se o arquivo não existir ou falhar, retorna o valor inicial/neutro
        return 0.5


class CalculateNodeReputationTask(ServiceHandlerBaseService):
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
            self.response.payload = {"status": "error","message": "no_id"}
            return

        # Calcula a própria reputação
        rep_result = self.invoke('soft-iot.reputation.orchestrator', {'node_id': my_gateway_id})

        if rep_result.get('status') == 'success':
            my_current_reputation = rep_result.get('reputation')
            
            self._save_reputation_to_file(my_current_reputation)
            
            self.logger.info(f"SUCESSO: Minha reputação salva no arquivo local: {my_current_reputation}")

            self.response.payload = {"status": "success"}
    

class ChangeDisturbingNodeBehaviorTask(ServiceHandlerBaseService):
    """
    Task para alterar o comportamento de um nó do tipo Perturbador (Tipo 4).
    Versão otimizada: Consulta a reputação local em vez de recalcular.
    """
    name = 'soft-iot.reputation.task.change_disturbing_behavior'

    REPUTATION_THRESHOLD = 0.9

    def handle(self):
        self.logger.info("Executando ChangeDisturbingNodeBehaviorTask...")

        nt_manager = NodeTypeManager()

        if nt_manager.node_type != 4:
            self.response.payload = {"status": "ignored", "message": "not_disturbing_node"}
            return

        if getattr(nt_manager, 'behavior_changed', False):
            self.response.payload = {"status": "ignored", "message": "already_changed"}
            return

        reputation_value = self._get_my_reputation()
        
        self.logger.info(f"Reputação lida do armazenamento local: {reputation_value}")

        if reputation_value > self.REPUTATION_THRESHOLD:
            self.logger.info(f"Limiar de {self.REPUTATION_THRESHOLD} atingido. Alterando a flag de comportamento.")
            
            nt_manager.behavior_changed = True
            
            self.response.payload = {"status": "success", "behavior_changed": True}
        else:
            self.response.payload = {"status": "success", "behavior_changed": False}

        if getattr(nt_manager, 'behavior_changed', True):
            self.response.payload = {"message": "behavior_changed"}
            return