# -*- coding: utf-8 -*-

import os
import json
import datetime
import random
from datetime import datetime, timezone

from zato.server.service import Service

from soft_iot_node_type import NodeTypeManager
from soft_iot_gateway_state import GatewayStateManager


class GetGatewayStateService(Service):
    """
    Serviço de leitura (Read Endpoint) que expõe o estado atual do Gateway.
    Recupera os dados persistidos no banco local e os serializa num Data Transfer Object (DTO).
    """
    name = 'soft-iot.gateway.state.get'

    def handle(self):
        self.logger.info("Executando GetGatewayStateService para extração completa de estado local...")
        
        try:
            # 1. Instancia o DAO
            gs_manager = GatewayStateManager()
            
            # 2. Coleta a totalidade dos dados das duas tabelas
            properties_data = gs_manager.get_all_properties()
            requests_data = gs_manager.get_all_requests()
            
            # 3. Monta o DTO hierárquico para a resposta (Payload)
            self.response.payload = {
                "status": "success",
                "data": {
                    "gateway_properties": properties_data,
                    "requests_history": requests_data,
                    "metrics": {
                        "total_requests_logged": len(requests_data)
                    }
                }
            }
            
            self.logger.info(f"Dados exportados com sucesso. Total de requisições enviadas: {len(requests_data)}")
            
        except Exception as e:
            self.logger.error(f"Erro crítico ao recuperar o estado do gateway: {e}")
            self.response.payload = {
                "status": "error",
                "message": "Falha interna ao ler a base de dados local."
            }

           
class CalculateNodeReputationTask(Service):
    """
    Calcula a reputação do próprio Gateway baseado nas avaliações da Tangle.
    """
    name = 'soft-iot.reputation.task.calculate'

    def handle(self):
        self.logger.info("Executando CalculateNodeReputationTask...")

        identity_info = self.invoke('soft-iot.id.manager')
        my_gateway_id = identity_info.get('gateway_id')

        gs_manager = GatewayStateManager()

        if not my_gateway_id:
            # Retorna o valor sob demanda (a escrita desnecessária de consenso na Tangle foi removida)
            self.response.payload = {"status": "error","message": "no_id"}
            return

        # Calcula a própria reputação
        rep_result = self.invoke('soft-iot.reputation.orchestrator', {'node_id': my_gateway_id})

        if rep_result.get('status') == 'success':
            my_current_reputation = rep_result.get('reputation')
            
            gs_manager.set_reputation(my_current_reputation)
            
            self.logger.info(f"SUCESSO: Minha reputação salva no arquivo local: {my_current_reputation}")

            self.response.payload = {"status": "success"}
    

class ChangeDisturbingNodeBehaviorTask(Service):
    """
    Task para alterar o comportamento de um nó do tipo Perturbador (Tipo 4).
    Versão otimizada: Consulta a reputação local em vez de recalcular.
    """
    name = 'soft-iot.reputation.task.change_disturbing_behavior'

    REPUTATION_THRESHOLD = 0.9

    def handle(self):
        self.logger.info("Executando ChangeDisturbingNodeBehaviorTask...")

        nt_manager = NodeTypeManager()
        gs_manager = GatewayStateManager()

        if nt_manager.node_type != 4:
            self.response.payload = {"status": "ignored", "message": "not_disturbing_node"}
            return

        if  gs_manager.get_behavior_changed():
            self.response.payload = {"status": "ignored", "message": "already_changed"}
            return

        reputation_value = gs_manager.get_reputation()
        
        self.logger.info(f"Reputação lida do armazenamento local: {reputation_value}")

        if reputation_value > self.REPUTATION_THRESHOLD:
            self.logger.info(f"Limiar de {self.REPUTATION_THRESHOLD} atingido. Alterando a flag de comportamento.")
            
            gs_manager.set_behavior_changed(True)
            
            self.response.payload = {"status": "success", "behavior_changed": True}
        else:
            self.response.payload = {"status": "success", "behavior_changed": False}
        

class CheckNodesServicesTask(Service):
    """
    Sorteia um serviço de interesse e publica uma requisição de busca (Broadcast) na Tangle.
    """
    name = 'soft-iot.reputation.task.check_nodes_services'

    def handle(self):
        self.logger.info("Executando CheckNodesServicesTask (Requisição de Serviços)...")

        gs_manager = GatewayStateManager()

        last_id, last_status = gs_manager.get_last_request_status()

        # 1. Bloqueio de Concorrência: Verifica se já existe um pedido em andamento
        if last_status == "WAITING_RESPONSES" or last_status == "REQUESTING SERVICE":

            start_time_str = gs_manager.get_start_request_time(last_id)

            if start_time_str:
            
                start_time_naive = datetime.strptime(start_time_str, '%Y-%m-%d %H:%M:%S')
                start_time_aware = start_time_naive.replace(tzinfo=timezone.utc)
                current_time_aware = datetime.now(timezone.utc)
                time_difference = current_time_aware - start_time_aware

                if time_difference.total_seconds() > 120:
                    self.logger.warning(f"TIMEOUT: A requisição {last_id} excedeu o limite!")
                    
                    gs_manager.update_request_status(last_id, 'TIMEOUT')
                    
                else:
                    self.logger.info(f"Requisição {last_id} em andamento.")
                    self.response.payload = {"status": "Already requesting"}
                    return
        
        # Lista fixa de serviços e cálculo de escolha aleatória
        available_services = [
            "temperatureSensor",
            "humiditySensor",
            "pulseOxymeter",
            "windDirectionSensor"
        ]

        chosen_service = random.choice(available_services)
        self.logger.info(f"Serviço escolhido aleatoriamente para requisição: {chosen_service}")
    
        # Recolhe a identidade do Gateway local
        identity_info = self.invoke('soft-iot.id.manager')
        my_gateway_id = identity_info.get('gateway_id')
        my_group = identity_info.get('group')

        if not my_gateway_id:
            self.logger.error("Falha: ID do Gateway não configurado.")
            self.response.payload = {"status": "ID not found"}
            return 
        
        request_id = gs_manager.create_request(my_gateway_id)


        # Estrutura do payload da requisição 
        request_transaction = {
            "source": my_gateway_id,
            "group": my_group,
            "type": "REP_SVC_REQ",
            "requestedService": chosen_service,
            "idRequest": request_id
        }

        # Publicação na Tangle utilizando o índice acordado para requisições
        broadcast_index = 'REP_HAS_SVC'
        
        try:
            res = self.invoke('soft-iot.dlt.client.api.write', {
                "index": broadcast_index,
                "data": request_transaction
            })

            if res.get("status") == "success":
                self.logger.info(f"Requisição do serviço {chosen_service} publicada no índice '{broadcast_index}'.")
            
                # Dispara o loop de contagem no Zato de forma assíncrona (não bloqueia esta task)
                #self.invoke_async('soft-iot.reputation.task.wait_nodes_responses', {})

                self.response.payload = {"status": "Request created"}
                return 
            
            else:
                self.logger.warning(f"Falha na rede ao publicar requisição: {res}")
                gs_manager.update_request_status(request_id, 'FAILED')
                self.response.payload = {"status": "Tangle write failed", "tangle_response": res}
                return
                
        except Exception as e:
            self.logger.error(f"Erro crítico ao solicitar serviço: {e}")
            gs_manager.update_request_status(request_id, 'FAILED')
            self.response.payload = {"status": "Internal Error"}
            return


