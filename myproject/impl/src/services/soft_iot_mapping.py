# -*- coding: utf-8 -*-

import json
import os
from zato.server.service import Service

# Caminho para o arquivo de configuração.
# O Zato roda a partir da pasta do servidor, então podemos usar caminho relativo ou absoluto.
CONFIG_FILE_PATH = '/home/ubuntu/mapping_archives/devices_config/devices.json'

class BaseMappingService(Service):
    """ Classe base com utilitários para ler o arquivo de dispositivos. """
    
    def _load_devices_from_file(self):
        """ Lê e parseia o arquivo JSON de dispositivos. """
        try:
            if not os.path.exists(CONFIG_FILE_PATH):
                self.logger.warning(f"Arquivo de dispositivos não encontrado em: {CONFIG_FILE_PATH}")
                return []

            with open(CONFIG_FILE_PATH, 'r') as f:
                data = json.load(f)
                return data
        except Exception as e:
            self.logger.error(f"Erro ao ler devices.json: {e}")
            return []

class GetDevice(BaseMappingService):
    """
    Retorna a configuração completa de um dispositivo específico.
    Equivalente ao método 'getDeviceById' do Java.
    """
    name = 'soft-iot.mapping.get-device'

    def handle(self):
        # Obtém o ID do payload da requisição (ex: {'device_id': 'sensor1'})
        device_id = self.request.payload.get('device_id')
        
        if not device_id:
            self.response.payload = {'error': 'device_id is required', 'device': None}
            return

        devices = self._load_devices_from_file()
        
        # Busca o dispositivo na lista
        target_device = next((d for d in devices if d.get('id') == device_id), None)

        if target_device:
            self.response.payload = {'status': 'found', 'device': target_device}
        else:
            self.response.payload = {'status': 'not_found', 'device': None}

class ListDevices(BaseMappingService):
    """
    Retorna todos os dispositivos cadastrados.
    Equivalente ao método 'getListDevices' do Java.
    """
    name = 'soft-iot.mapping.list-devices'

    def handle(self):
        devices = self._load_devices_from_file()
        self.response.payload = {'total': len(devices), 'devices': devices}