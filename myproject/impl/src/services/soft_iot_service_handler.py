# -*- coding: utf-8 -*-

import os
import json
import datetime
import random
from zato.server.service import Service
from soft_iot_node_type import NodeTypeManager


class ServiceHandlerBaseService(Service):
    """
    Classe base de acesso ao arquivo de armazenamento da reputação local. 
    """

    STATE_FILE = '/tmp/gateway_data.json'

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
        state_data['my_reputation'] = reputation_value
        
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
                    return float(state_data.get('my_reputation', 0.5))
            except Exception:
                pass
        
        # Se o arquivo não existir ou falhar, retorna o valor inicial/neutro
        return 0.5

    def _update_offline_status(self, offline_list):
        """
        Salva a lista de IDs de dispositivos offline, preservando outros dados.
        """
        data = {}
        if os.path.exists(self.STATE_FILE):
            try:
                with open(self.STATE_FILE, 'r') as f:
                    data = json.load(f)
            except Exception as e:
                self.logger.error(f"Erro na leitura para atualização de status: {e}")
        
        data['offline_devices'] = offline_list
        
        try:
            with open(self.STATE_FILE, 'w') as f:
                json.dump(data, f, indent=4)
        except Exception as e:
            self.logger.error(f"Falha ao gravar dispositivos offline: {e}")

    def _get_offline_devices(self):
        """
        Recupera a lista de IDs de dispositivos offline do armazenamento local.
        """
        if os.path.exists(self.STATE_FILE):
            try:
                with open(self.STATE_FILE, 'r') as f:
                    return json.load(f).get('offline_devices', [])
            except Exception:
                pass
        return []
    
    
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
        

class CheckDevicesTask(ServiceHandlerBaseService):
    """
    Monitora a conectividade dos nós (dispositivos) baseada na atividade dos sensores.
    Um dispositivo é offline apenas se todos os seus sensores estiverem inativos.
    """
    name = 'soft-iot.reputation.task.check_devices'
    OFFLINE_THRESHOLD = 60 # 5 minutos

    def handle(self):
        self.logger.info("Iniciando auditoria de conectividade dos dispositivos...")

        # 1. Lista todos os dispositivos registrados no mapeamento
        mapping_res = self.invoke('soft-iot.mapping.list-devices')
        devices = mapping_res.get('devices', [])
        
        now = datetime.datetime.now()
        offline_devices = []

        for device in devices:
            device_id = device.get('id')
            sensors = device.get('sensors', [])
            
            # Assume inicialmente que o dispositivo está offline
            is_device_online = False

            for sensor in sensors:
                sensor_id = sensor.get('id')
                
                # 2. Verifica a última atividade do sensor específico
                last_data = self.invoke('soft-iot.api.get-last-sensor-data', {
                    'device_id': device_id, 
                    'sensor_id': sensor_id
                })

                self.logger.error(f"Dados do sensor: {last_data.get('data')}")
                
                item = last_data.get('data')
                
                if item:
                    try:
                        # Extrai timestamp retornado pela API (formato YYYY-MM-DD HH:MM:SS.mmmmmm)
                        last_seen = datetime.datetime.fromisoformat(item.get('end_datetime'))
                        diff = (now - last_seen).total_seconds()

                        # 3. Se um único sensor estiver ativo, o dispositivo todo está online
                        if diff <= self.OFFLINE_THRESHOLD:
                            is_device_online = True
                            self.logger.info(f"Dispositivo {device_id} ativo via sensor {sensor_id}.")
                            break 
                    except (ValueError, TypeError):
                        continue

            # 4. Se após checar todos os sensores nenhum estiver ativo, marca o dispositivo como offline
            if not is_device_online and device_id not in offline_devices:
                self.logger.error(f"Dispositivo {device_id} classificado como OFFLINE.")
                offline_devices.append(device_id)

        # 5. Persiste a lista apenas com os IDs dos dispositivos
        self._update_offline_status(offline_devices)


class GetGatewayStateService(ServiceHandlerBaseService):
    """
    Serviço REST para retornar todos os dados armazenados no arquivo de estado local do Gateway.
    """
    name = 'soft-iot.reputation.state.get_all'

    def handle(self):
        self.logger.info("Recebida requisição para auditoria do estado local do Gateway.")

        # Verifica se o arquivo de estado já foi criado por alguma das tasks
        if not os.path.exists(self.STATE_FILE):
            self.logger.warning("Arquivo de estado não encontrado. Retornando estado vazio.")
            self.response.payload = {
                "status": "success", 
                "data": {}
            }
            return

        # Executa a leitura e desserialização do JSON
        try:
            with open(self.STATE_FILE, 'r') as f:
                state_data = json.load(f)
            
            self.response.payload = {
                "status": "success",
                "data": state_data
            }
            
        except json.JSONDecodeError as e:
            self.logger.error(f"Erro ao decodificar o arquivo de estado (JSON malformado): {e}")
            self.response.status_code = 500
            self.response.payload = {
                "status": "error",
                "message": "O arquivo de estado local está corrompido ou malformado."
            }
        except Exception as e:
            self.logger.error(f"Erro de I/O ao ler arquivo de estado local: {e}")
            self.response.status_code = 500
            self.response.payload = {
                "status": "error",
                "message": "Falha interna ao acessar o arquivo de estado."
            }