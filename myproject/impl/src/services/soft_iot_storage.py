# -*- coding: utf-8 -*-

# Zato
from zato.server.service import Service

# Standard library
import threading
import sqlite3
import json
import time
import os
import logging
import random
from datetime import datetime

# Third-party
import paho.mqtt.client as mqtt

# Extended TATU Wrapper (certifique-se que está instalado via pip)
try:
    from extended_tatu_wrapper.utils import tatu_wrapper
    from extended_tatu_wrapper.enums import ExtendedTATUMethods
except ImportError:
    # Fallback ou log de erro critico se a lib não estiver presente
    logging.error("CRITICAL: extended_tatu_wrapper not found. Please install it via pip.")
    tatu_wrapper = None

# --- Configurações Hardcoded (como solicitado) ---
BROKER_URL = "0.0.0.0" # Ou o nome do container do broker, ex: "mosquitto"
BROKER_PORT_TCP = 1883
BROKER_PORT_WS = 9001
DB_FILENAME = "/opt/zato/env/soft_iot_data.db" # Caminho persistente dentro do container Zato
DEFAULT_COLLECTION_TIME = int(os.getenv('Zato_COLLECTION_TIME', 5))
DEFAULT_PUBLISHING_TIME = int(os.getenv('Zato_PUBLISH_TIME', 15))


# --- Controlador Singleton do MQTT e Banco de Dados ---

class LocalStorageController:
    _instance = None
    _lock = threading.Lock()

    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super(LocalStorageController, cls).__new__(cls)
                    cls._instance.is_running = False
                    cls._instance.client = None
        return cls._instance

    def start(self, logger):
        """Inicia o loop MQTT se ainda não estiver rodando."""
        if self.is_running:
            return

        self.logger = logger
        self.logger.info("Iniciando Soft-IoT Local Storage Controller...")
        
        #Inicializa memória temporária para auto-cadastro
        self.pending_registration = {}

        # 1. Inicializar Banco de Dados
        self.init_db()

        # 2. Configurar e Conectar MQTT
        self.connect_mqtt()
        
        self.is_running = True

    def init_db(self):
        """Cria as tabelas SQLite se não existirem."""
        try:
            with sqlite3.connect(DB_FILENAME) as conn:
                cursor = conn.cursor()
                
                # Tabela principal de dados
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS sensor_data (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        sensor_id TEXT,
                        device_id TEXT,
                        data_value TEXT,
                        start_datetime TIMESTAMP,
                        end_datetime TIMESTAMP,
                        aggregation_status INTEGER DEFAULT 0
                    )
                ''')
                
                # Tabelas auxiliares (mantidas do original Java para compatibilidade futura)
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS semantic_registered_last_time_sensors (
                        sensor_id TEXT,
                        device_id TEXT,
                        last_time TIMESTAMP
                    )
                ''')
                
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS aggregation_registered_last_time_sensors (
                        sensor_id TEXT,
                        device_id TEXT,
                        last_time TIMESTAMP
                    )
                ''')
                conn.commit()
            self.logger.info(f"Banco de dados SQLite inicializado em {DB_FILENAME}")
        except Exception as e:
            self.logger.error(f"Erro ao inicializar banco de dados: {e}")

    def connect_mqtt(self):
        """Configura o cliente MQTT Paho."""
        client_id = f"SoftIoT_Storage_{int(time.time())}"
        self.client = mqtt.Client(client_id=client_id)
        
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        self.client.on_disconnect = self.on_disconnect

        try:
            self.logger.info(f"Conectando ao broker MQTT {BROKER_URL}:{BROKER_PORT_TCP}...")
            self.client.connect(BROKER_URL, BROKER_PORT_TCP, 60)
            self.client.loop_start() # Executa em uma thread separada gerenciada pela lib
        except Exception as e:
            self.logger.error(f"Falha ao conectar no MQTT: {e}")

    def on_connect(self, client, userdata, flags, rc):
        self.logger.info(f"MQTT Conectado com código: {rc}")
        topics = [
            ("CONNECTED", 1),       # Legado (Java)
            ("dev/CONNECTIONS", 1), # Novo padrão (Python Device)
            ("dev/#", 1)            # Dados e Respostas
        ]
        client.subscribe(topics)
        self.logger.info(f"Inscrito nos tópicos: {topics}")

    def on_disconnect(self, client, userdata, rc):
        self.logger.warning("MQTT Desconectado. O Paho tentará reconectar automaticamente se loop_start foi usado.")

    def on_message(self, client, userdata, msg):
        try:
            topic = msg.topic
            payload = msg.payload.decode('utf-8').strip()

            if not payload or tatu_wrapper is None:
                return

            # Ignorar mensagens de controle/resposta do próprio gateway
            if topic == "dev/CONNECTIONS/RES": 
                return # Ignora apenas a confirmação de conexão

            # Tratar Handshake
            if topic == "dev/CONNECTIONS":
                self.handle_connect_request(payload)
                return 

            # Tratar Respostas de Dados
            if tatu_wrapper.is_tatu_response(payload):
                self.handle_tatu_response(payload)

        except Exception as e:
            self.logger.error(f"Erro no processamento MQTT: {e}")


    def handle_connect_request(self, payload):
        """
        Processa o CONNECT do Virtual-FoT-Device e envia o CONNACK.
        """
        self.logger.info(f"DEBUG PAYLOAD (Len: {len(payload)}): '{payload}'")

        if not payload:
            self.logger.warning("Payload vazio recebido no CONNECT. Ignorando.")
            return

        try:
            # --- CORREÇÃO: Extrair JSON do comando TATU ---
            # O formato é "CONNECT VALUE BROKER {...}"
            # Localizamos onde começa o objeto JSON
            json_start_index = payload.find('{')
            
            if json_start_index == -1:
                self.logger.error("Payload CONNECT inválido: Nenhum objeto JSON encontrado.")
                return
                
            # Cortamos a string para pegar apenas do '{' em diante
            clean_payload = payload[json_start_index:]
            data = json.loads(clean_payload)
            # ---------------------------------------------
            
            header = data.get("HEADER", {})
            device_id = header.get("NAME")
            
            #Captura dados da estrutura do dispositivo para auto-cadastro ---
            device_structure = data.get("DEVICE")
            if device_id and device_structure:
                self.logger.info(f"Estrutura do dispositivo {device_id} capturada para possível cadastro.")
                self.pending_registration[device_id] = device_structure


            if not device_id:
                self.logger.warning("Recebido CONNECT sem nome do dispositivo.")
                return

            self.logger.info(f"Recebido CONNECT de {device_id}. Enviando CONNACK...")

            # Monta resposta CONNACK (Autorizando conexão)
            connack = {
                "CODE": "POST",
                "METHOD": "CONNACK",
                "HEADER": {
                    "NAME": "Zato-Gateway",
                    "TIMESTAMP": int(time.time() * 1000)
                },
                "BODY": {
                    "NEW_NAME": device_id,
                    "CAN_CONNECT": True
                }
            }
            
            # Publica no tópico de resposta esperado pelo dispositivo
            resp_topic = "dev/CONNECTIONS/RES"
            self.client.publish(resp_topic, json.dumps(connack))
            self.logger.info(f"CONNACK enviado para {resp_topic}")
            
            # Inicia o fluxo de dados
            time.sleep(0.5) 
            self.handle_device_connected(device_id)

        except Exception as e:
            self.logger.error(f"Erro ao processar CONNECT: {e}")

    def handle_tatu_response(self, message_content):
        """Processa respostas TATU (GET ou FLOW) e salva no banco."""
        try:
            device_id = tatu_wrapper.get_device_id_by_tatu_answer(message_content)
            sensor_id = tatu_wrapper.get_sensor_id_by_tatu_answer(message_content)
            
            # Parsing manual dos dados pois o wrapper Python fornecido é mais simples que o Java
            data_list = self.parse_tatu_body_to_data(message_content)
            
            if data_list:
                self.logger.info(f"Armazenando {len(data_list)} registros para Dev: {device_id}, Sensor: {sensor_id}")
                self.store_sensor_data(device_id, sensor_id, data_list)
            else:
                self.logger.warning("Resposta TATU válida mas sem dados extraíveis.")

        except Exception as e:
            self.logger.error(f"Erro ao processar resposta TATU: {e}")

    def parse_tatu_body_to_data(self, json_str):
        """
        Reimplementação da lógica Java 'TATUWrapper.parseTATUAnswerToListSensorData'.
        O wrapper python fornecido não tem esse helper, então criamos aqui.
        """
        data_points = []
        try:
            obj = json.loads(json_str)
            body = obj.get("BODY", {})
            # O timestamp do cabeçalho serve como base
            header_ts = int(obj.get("HEADER", {}).get("TIMESTAMP", time.time()*1000))
            
            for key, value in body.items():
                if key == "FLOW": continue # Ignora metadados de fluxo
                
                # O valor pode ser um único valor ou uma lista (FLOW)
                # Ex: "temperature": 25.5 ou "temperature": [25.5, 26.0, ...]
                
                values = value if isinstance(value, list) else [value]
                
                # Se for lista, precisamos inferir o tempo. 
                # Na implementação Java original, isso é complexo e depende da collection_time.
                # Aqui, para simplificar a conversão, usaremos o timestamp do header 
                # para o último dado e retrocederemos (ou usaremos o mesmo para todos se for GET).
                
                # Assumindo comportamento padrão do TATU:
                # Se GET, é valor instantâneo.
                # Se FLOW, são valores históricos.
                
                method = obj.get("METHOD")
                
                if method == "GET":
                    data_points.append({
                        "value": str(values[0]),
                        "timestamp": datetime.fromtimestamp(header_ts / 1000.0)
                    })
                else:
                    # Lógica simplificada para FLOW: 
                    # Se tivermos collection_time no BODY->FLOW, usamos. Senão, default.
                    flow_info = body.get("FLOW", {})
                    collect_ms = flow_info.get("collect", DEFAULT_COLLECTION_TIME * 1000) 
                    if collect_ms < 100: collect_ms = DEFAULT_COLLECTION_TIME * 1000 # Sanity check

                    # Itera reverso para calcular timestamps
                    current_ts = header_ts
                    for v in reversed(values):
                        data_points.append({
                            "value": str(v),
                            "timestamp": datetime.fromtimestamp(current_ts / 1000.0)
                        })
                        current_ts -= collect_ms
                    
                    data_points.reverse() # Volta para ordem cronológica

        except Exception as e:
            self.logger.error(f"Erro no parsing JSON do TATU: {e}")
        
        return data_points

    def store_sensor_data(self, device_id, sensor_id, data_list):
        """Insere dados no SQLite (com verificação de variável de ambiente)."""
        
        # --- NOVO: Verifica se deve salvar ---
        # Padrão é 'true' se a variável não existir
        save_enabled = os.getenv('Zato_SAVE_DATA_ENABLED', 'True')
        if(save_enabled == 'False'):
            save_enabled = False
        else:
            save_enabled = True
            
        if not save_enabled:
            # Apenas loga (opcional, para não poluir muito se for alta frequência)
            self.logger.info(f"[PERSISTÊNCIA OFF] Ignorando {len(data_list)} registros de {device_id}/{sensor_id}")
            return
        

        try:
            with sqlite3.connect(DB_FILENAME) as conn:
                cursor = conn.cursor()
                sql = '''
                    INSERT INTO sensor_data (sensor_id, device_id, data_value, start_datetime, end_datetime, aggregation_status)
                    VALUES (?, ?, ?, ?, ?, 0)
                '''
                # Prepara batch
                batch = []
                for item in data_list:
                    # Garante que o timestamp seja string ISO ou int, conforme sua lógica de banco
                    timestamp = item['timestamp'] 
                    
                    batch.append((
                        sensor_id, 
                        device_id, 
                        str(item['value']), # Convertendo para string por segurança (SQLite é dinâmico)
                        timestamp, 
                        timestamp
                    ))
                
                if batch:
                    cursor.executemany(sql, batch)
                    conn.commit()
                    self.logger.info(f"Salvos {len(batch)} registros para {device_id}/{sensor_id}")
                    
        except Exception as e:
            self.logger.error(f"Erro SQL ao inserir dados: {e}")

    def handle_device_connected(self, device_id):
        self.logger.info(f"ENTREI NO HANDLE_DEVICE_CONNECTED PARA O ID: '{device_id}'")
        """Envia requisição de FLOW quando um dispositivo conecta."""
        self.logger.info(f"Dispositivo conectado detectado: {device_id}")
        
        try:
            CONFIG_FILE_PATH = '/home/ubuntu/mapping_archives/devices_config/devices.json'
            
            # 1. Carregar dispositivos existentes
            devices = []
            if os.path.exists(CONFIG_FILE_PATH):
                 with open(CONFIG_FILE_PATH, 'r') as f:
                     try:
                         devices = json.load(f)
                     except json.JSONDecodeError:
                         devices = []

            # 2. Buscar dispositivo
            device_info = next((d for d in devices if d.get('id') == device_id), None)
            
            # Lógica de Auto-Cadastro 
            if not device_info:
                self.logger.warning(f"Dispositivo {device_id} não encontrado. Verificando auto-cadastro...")
                if device_id in self.pending_registration:
                    new_device_data = self.pending_registration.pop(device_id)
                    new_device_data['id'] = device_id 
                    for sensor in new_device_data.get('sensors', []):
                        sensor['collection_time'] = DEFAULT_COLLECTION_TIME
                        sensor['publishing_time'] = DEFAULT_PUBLISHING_TIME
                    
                    devices.append(new_device_data)
                    self._save_devices_file(CONFIG_FILE_PATH, devices)
                    device_info = new_device_data
                    self.logger.info(f"Dispositivo {device_id} auto-cadastrado com sucesso!")
                else:
                    self.logger.error(f"Sem dados para auto-cadastrar {device_id}.")
                    return

            # 3. Envio dos comandos FLOW
            if device_info:
                sensors = device_info.get('sensors', [])
                self.logger.info(f"Iniciando configuração de {len(sensors)} sensores para {device_id}...")
                
                for sensor in sensors:
                    # 1. Garante que o ID do sensor seja uma string limpa (sem espaços ou quebras de linha)
                    sensor_id = str(sensor.get('id')).strip()
                    
                    c_time = int(sensor.get('collection_time', DEFAULT_COLLECTION_TIME) * 1000)
                    p_time = int(sensor.get('publishing_time', DEFAULT_PUBLISHING_TIME) * 1000)
                    
                    # 2. JSON compacto 
                    flow_body = json.dumps({"collect": c_time, "publish": p_time}, separators=(',', ':'))
                    
                    # 3. Formata a string garantindo espaços ÚNICOS entre os tokens
                    flow_req = f"FLOW VALUE {sensor_id} {flow_body}"
                    
                    topic = f"dev/{device_id}"
                    self.logger.info(f"Enviando para o Java (ID exato): '{sensor_id}'")
                    
                    self.client.publish(topic, flow_req)
                    time.sleep(0.2)
                
                self.logger.info(f"Todos os comandos de fluxo enviados para {device_id}")

        except Exception as e:
            self.logger.error(f"Erro ao tentar configurar dispositivo conectado: {e}")

    # --- NOVO: Helper para salvar JSON ---
    def _save_devices_file(self, filepath, data):
        """Salva a lista de dispositivos no arquivo JSON."""
        try:
            # Garante que o diretório existe
            os.makedirs(os.path.dirname(filepath), exist_ok=True)
            with open(filepath, 'w') as f:
                json.dump(data, f, indent=4)
        except Exception as e:
            self.logger.error(f"Erro ao salvar arquivo de dispositivos: {e}")


# --- Serviço Zato ---

class SoftIoTStorageService(Service):
    """
    Serviço Zato responsável por inicializar o controlador de armazenamento local.
    Este serviço deve ser configurado no Zato Scheduler para rodar 'On Startup'.
    """
    name = 'soft-iot.storage.service'

    def handle(self):
        # Usa o logger nativo do Zato
        controller = LocalStorageController()
        
        # Verifica se já está rodando (Singleton)
        if not controller.is_running:
            self.logger.info("Inicializando o controlador Soft-IoT Storage...")
            controller.start(self.logger)
        else:
            self.logger.info("Controlador Soft-IoT Storage já está rodando.")