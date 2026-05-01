# -*- coding: utf-8 -*-

import threading
import json
import os
import logging

# A MÁGICA ESTÁ AQUI: importando a versão compatível com Gevent do Zato
import zmq.green as zmq

from zato.server.service import Service

logger = logging.getLogger('zato.zmq') 

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


class ZMQStartupService(Service):
    """
    Serviço responsável por ativar o Singleton do ZMQ Manager.
    """

    name = 'soft-iot.zmq.start'

    def handle(self):
        self.logger.info("Recebida requisição para iniciar ZMQ Manager.")
        manager = ZMQManager()
        self.logger.info(f"ZMQ Manager status atual: {'operando' if manager.is_running else 'parado'}")
        
        if not manager.is_running:
            # Chama o start() direto, pois ele já se encarrega de criar a sua própria Thread
            manager.start()
            self.logger.info("Comando de startup ZMQ enviado.")
        else:
            self.logger.info("ZMQ Manager ja esta operando.")

        self.response.payload = {"status": "ok", "message": "Startup iniciado"}