# -*- coding: utf-8 -*-

import os
import requests

from zato.server.service import Service


class DLTClientBaseService(Service):
    """
    Classe base de configurações. 
    """

    def _init_configs(self):

        # Leitura do IP e da porta da API pelas variáveis de ambiente. Tem como default: localhost:3000
        self.api_ip = os.environ.get('Zato_TANGLE_API_IP', '127.0.0.1')
        self.api_port = os.environ.get('Zato_TANGLE_API_PORT', '3000')
        self.base_url = f"http://{self.api_ip}:{self.api_port}"

    def _ensure_configs(self):
        """Garante que a base_url existe antes de qualquer operação."""

        if not hasattr(self, 'base_url'):
            self._init_configs()


class DLTWriterService(DLTClientBaseService):
    """
    Serviço de escrita na Tangle.Nome do Serviço Zato	URL Sugerida (url_path)	Método	Descrição
soft-iot.dlt.client.api.write	/soft-iot/dlt/transactions	POST	Envia um novo dado para ser gravado na Tangle.
soft-iot.dlt.client.api.read_index
    """

    name = 'soft-iot.dlt.client.api.write'


    def handle(self):
        """
        Escrita de mensagem.
        """

        # Extração dos dados da requisição.
        request_data = self.request.payload
        index = request_data.get('index')             # Índice.
        transaction_data = request_data.get('data')   # Dados.

        self._ensure_configs()

        # Validação básico, caso não haja dados.
        if not index or not transaction_data:
            self.logger.error("Index ou data ausentes no payload.")
            self.response.payload = {"status": "error", "message": "Missing index or data"}
            return

        url = f"{self.base_url}/message"     # Endpoint de escrita na Tangle.
        payload = {'index': index, 'data': transaction_data}

        try:
            # Fazendo chamada da rota de escrita da API.
            response = requests.post(url, json=payload, timeout=10)

            if response.status_code in (200, 201):
                self.logger.info(f"RESPOSTA DA ESCRITA: {response.text}") 
                
                # De a API retornou um JSON válido, entrega a resposta do Zato.
                if response.text and response.text.strip():
                    try:
                        self.response.payload = response.json()
                    except ValueError:
                        self.response.payload = {"status": "ok", "message": "Accepted"}
                else:
                    self.response.payload = {"status": "ok"}

            else:
                self.logger.error(f"Erro na API Hornet ({response.status_code}): {response.text}")
                self.response.payload = {"status": "error", "code": response.status_code}
                
        except Exception as e:
            self.logger.error(f"Falha na requisição para {url}: {e}")
            self.response.payload = {"status": "error", "message": str(e)}



class DLTIndexReaderService(DLTClientBaseService):
    """
    Serviço de busca de transações por índice (Substitui LedgerReader.java).
    """
    name = 'soft-iot.dlt.client.api.read_index'

    def handle(self):
        """
        Leitura de mensagens por índice.
        """

        self._ensure_configs()

        # Coleta do índice pelo corpo ou pelos parâmetros da URL.
        request_data = self.request.payload or self.request.params
        index = request_data.get('index')

        if not index:
            self.logger.error("Índice não fornecido para busca.")
            self.response.payload = []
            return

        # Endpoint de busca das mensagens por índice.
        url = f"{self.base_url}/message/{index}"   
        
        try:
            response = requests.get(url, timeout=10)
            
            if response.status_code == 200:

                # Retorna a lista em formato JSON, caso as mensagens sejam encontradas.
                if response.text and response.text.strip():
                    try:
                        self.response.payload = response.json()
                    except ValueError:
                        self.logger.warning(f"Resposta inválida do Hornet para o índice {index}")
                        self.response.payload = []

                else:
                    self.response.payload = []

            else:
                self.logger.error(f"Erro na API Hornet ({response.status_code}) ao ler índice {index}")
                self.response.payload = []

        except Exception as e:
            self.logger.error(f"Falha na leitura do índice {index}: {e}")
            self.response.payload = []


class DLTIdReaderService(DLTClientBaseService):
    """
    Busca uma transação específica pelo ID (Substitui LedgerReader.java).
    """
    name = 'soft-iot.dlt.client.api.read_id'

    def handle(self):
        """
        Busca mensagem por ID.
        """
        
        self._ensure_configs()

        request_data = self.request.payload or self.request.params
        message_id = request_data.get('message_id')     # Coleta do ID da mensagem

        if not message_id:
            self.logger.error("Message ID não fornecido para busca.")
            self.response.payload = None
            return

        # Endpoint de busca da transação pelo ID.
        url = f"{self.base_url}/message/messageId/{message_id}"
        
        try:

            response = requests.get(url, timeout=10)
            
            if response.status_code == 200:

                if response.text and response.text.strip():

                    try:
                        self.response.payload = response.json()

                    except ValueError:
                        self.logger.warning(f"Resposta JSON inválida para o ID: {message_id}")
                        self.response.payload = None

                else:
                    self.response.payload = None

            else:
                self.logger.error(f"Erro na API Hornet ({response.status_code}) ao buscar ID: {message_id}")
                self.response.payload = None

        except Exception as e:
            self.logger.error(f"Falha na requisição de busca por ID {message_id}: {e}")
            self.response.payload = None