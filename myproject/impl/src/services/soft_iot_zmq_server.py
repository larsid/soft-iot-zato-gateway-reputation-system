# -*- coding: utf-8 -*-

import threading
import json
import os
import logging

from soft_iot_id_manager import IDManager
from soft_iot_gateway_state import GatewayStateManager

import zmq.green as zmq
import requests

from zato.server.service import Service

logger = logging.getLogger('zato.zmq') 

CONFIG_FILE_PATH = '/home/ubuntu/mapping_archives/devices_config/devices.json'

class ZMQManager:
    """
    Controlador Singleton para gerenciar a conexão ZMQ de forma compatível com Zato.
    """

    _instance = None
    _lock = threading.Lock()

    def __new__(cls):
        logger.info("Criando instância do ZMQManager...")
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super(ZMQManager, cls).__new__(cls)
                    cls._instance.is_running = False
                    cls._instance.context = zmq.Context()
                    cls._instance.socket = None
                    cls._instance.subscribers = [] 

        return cls._instance 

    def _init_configs(self):
        self.zmq_ip = os.environ.get('Zato_ZMQ_IP', '127.0.0.1')
        self.zmq_port = os.environ.get('Zato_ZMQ_PORT', '5556')
        self.zmq_url = f"tcp://{self.zmq_ip}:{self.zmq_port}"

    def start(self):
        if self.is_running:
            return
        
        if not hasattr(self, 'zmq_url'):
            self._init_configs()
            
        logger.info(f"Iniciando ZMQ Server Thread conectando em: {self.zmq_url}")
    
        self.is_running = True

        self.thread = threading.Thread(target=self._run, name="ZMQServerThread")
        self.thread.daemon = True
        self.thread.start()

        logger.info(f"Start finalizado")
        return
        

    def _run(self):
        try:
            self.socket = self.context.socket(zmq.SUB)
            self.socket.connect(self.zmq_url)
            self.socket.setsockopt_string(zmq.SUBSCRIBE, "REP_") 
            
            logger.info(f"ZMQ conectado com sucesso em {self.zmq_url}")

            while self.is_running:
                logger.info(f"Iniciando leitura (Modo Green/Assíncrono)")
                
                # Agora esta função vai esperar a mensagem, mas sem travar o resto do Zato
                frames = self.socket.recv_multipart()

                for frame in frames:
                    raw_content = frame.decode('utf-8')
                    parts = raw_content.split(' ', 1)

                    if len(parts) < 2:
                        continue

                    topic_str = parts[0]
                    json_payload_str = parts[1]

                    try:
                        full_payload = json.loads(json_payload_str)
                        inner_data_str = full_payload.get('payload', {}).get('data', '{}')
                        app_data = json.loads(inner_data_str)

                        logger.info(f"Processando {topic_str} de: {app_data.get('source', 'desconhecido')}")
                        logger.info(f"Dados: {app_data}")

                        self._notify_subscribers(topic_str, app_data)
                    
                    except json.JSONDecodeError:
                        logger.error(f"Erro ao decodificar JSON do tópico {topic_str}")

        except Exception as e:
            logger.error(f"Erro crítico no loop ZMQ: {e}")
            self.is_running = False

    def _notify_subscribers(self, topic, payload):
        for sub in self.subscribers:
            try:
                sub.update(topic, payload)
            except Exception as e:
                logger.error(f"Erro ao notificar serviço: {e}")


class ServiceResponseSubscriber:
    """
    Ouvinte dedicado a interceptar respostas de requisições de serviço (REP_SVC_RES).
    """

    def update(self, topic, payload):

        if topic == "REP_HAS_SVC":
            msg_type = payload.get('type')
            
            # Lógica para receber respostas da requisição feita
            if msg_type == 'REP_SVC_RES':
                self._process_response(payload)

    def _process_response(self, payload):

        id_manager = IDManager()
        target = payload.get('target')

        # Verifica se o gateway atual é o alvo
        if target != id_manager.id:
            return

        # Extração dos dados
        id_request = payload.get('idRequest') 
        source = payload.get('source')
        ip_source = payload.get('ipSource')
        services = payload.get('services')
        group_name = payload.get('group')

        gs_manager = GatewayStateManager()
        
        # Filtro 4: Verifica o status da máquina de estados
        current_status = gs_manager.get_request_status(id_request)

        if current_status == 'WAITING_RESPONSES':
            logger.info(f"Recebida proposta válida do provedor {source} (IP: {ip_source}).")
            gs_manager.save_response(id_request, source, ip_source, target, services, group_name)


class ServiceRequestSubscriber:
    """
    Ouvinte dedicado a interceptar requisições (REP_SVC_REQ) provenientes do barramento.
    """

    def update(self, topic, payload):
        if topic == "REP_HAS_SVC":
            msg_type = payload.get('type')
            
            if msg_type == 'REP_SVC_REQ':
                self._process_request(payload)

    def _load_devices_from_file(self):
        """ 
        Lê e parseia o arquivo JSON de dispositivos.
        Replicação exata da lógica do BaseMappingService.
        """
        try:
            if not os.path.exists(CONFIG_FILE_PATH):
                logger.warning(f"Arquivo de dispositivos não encontrado em: {CONFIG_FILE_PATH}")
                return []

            with open(CONFIG_FILE_PATH, 'r', encoding='utf-8') as f:
                data = json.load(f)
                return data
        except Exception as e:
            logger.error(f"Erro ao ler devices.json no ZMQ: {e}")
            return []

    def _process_request(self, payload):
        requested_service = payload.get('requestedService')
        requester_id = payload.get('source')
        id_request = payload.get('idRequest')

        if not requested_service or not requester_id:
            logger.warning("Requisição recebida com parâmetros nulos.")
            return

        logger.info(f"Processando requisição de serviço '{requested_service}' do nó {requester_id} no ZMQ.")

        devices = self._load_devices_from_file()

        # Buscando sensores com serviço requisitado
        matched_services = []

        for device in devices:
            device_id = device.get('id')
            sensors = device.get('sensors', [])
            
            for sensor in sensors:
                if sensor.get('type') == requested_service:
                    matched_services.append({
                        "device_id": device_id,
                        "sensor_id": sensor.get('id')
                    })
        
        if matched_services:
            logger.info(f"Encontrados {len(matched_services)} sensores compatíveis. Preparando REP_SVC_RES.")
            
            id_manager = IDManager()
            my_gateway_id = id_manager.id
            my_group = id_manager.group
            my_ip = id_manager.ip

            response_tx = {
                "type": "REP_SVC_RES",
                "target": requester_id,
                "idRequest": id_request,
                "source": my_gateway_id,
                "ipSource": my_ip,
                "group": my_group,
                "services": matched_services
            }

            # Comunicação local via HTTP (Loopback) para acionar a escrita na DLT
            try:

                url_dlt = 'http://127.0.0.1:11223/soft-iot/dlt/transactions' 
                dlt_payload = {
                    "index": "REP_HAS_SVC",
                    "data": response_tx
                }
                
                dlt_response = requests.post(url_dlt, json=dlt_payload, timeout=10)
                
                if dlt_response.status_code == 200:
                    logger.info(f"Oferta de serviço publicada na Tangle com sucesso para o nó {requester_id}.")
                else:
                    logger.error(f"Falha na API local de DLT. Status: {dlt_response.status_code}")
                    
            except requests.exceptions.RequestException as e:
                logger.error(f"Erro de rede ao acionar a API local de DLT: {e}")
                
        else:
            logger.info(f"O nó local não possui o serviço '{requested_service}'. Ignorando.")


class ZMQStartupService(Service):
    """
    Serviço responsável por ativar o Singleton do ZMQ Manager e plugar os ouvintes.
    """
    name = 'soft-iot.zmq.start'

    def handle(self):
        self.logger.info("Recebida requisição para iniciar ZMQ Manager.")
        manager = ZMQManager()
        self.logger.info(f"ZMQ Manager status atual: {'operando' if manager.is_running else 'parado'}")
        
        if not manager.is_running:

            if not any(isinstance(sub, ServiceResponseSubscriber) for sub in manager.subscribers):
                subscriber = ServiceResponseSubscriber()
                manager.subscribers.append(subscriber)
                self.logger.info("ServiceResponseSubscriber registrado com sucesso.")

            manager.start()
            self.logger.info("Comando de startup ZMQ enviado.")
        else:
            self.logger.info("ZMQ Manager já está operando.")

        self.response.payload = {"status": "success", "message": "Startup concluído"}