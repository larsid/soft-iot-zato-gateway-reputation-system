# -*- coding: utf-8 -*-

import sqlite3
import logging

class GatewayStateManager:
    """
    Data Access Object (DAO) para gerir o estado concorrente do Gateway.
    Infraestrutura base com suporte a Write-Ahead Logging (WAL).
    """

    _STATE_KEY = 'gateway_state'

    def __init__(self, db_path='/tmp/gateway_state.db'):
        self.db_path = db_path
        self.logger = logging.getLogger('zato.gateway.state')
        # Garante que a base de dados existe assim que o módulo é instanciado
        self._init_db()

    def _get_connection(self):
        """
        Estabelece a conexão com o banco de dados SQLite.
        """
        # O parâmetro timeout=10.0 obriga o Worker a aguardar até 10 segundos 
        # caso o banco esteja momentaneamente trancado por outra transação, 
        # em vez de falhar imediatamente.
        conn = sqlite3.connect(self.db_path, timeout=10.0)
        
        # Ativação do modo WAL para concorrência de múltiplos processos
        conn.execute('pragma journal_mode=wal')
        
        return conn

    def _init_db(self):
        """
        Garante a criação das estruturas transacionais no disco e a inicialização do estado padrão.
        """
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                
                # 1. DDL: Criação da Tabela
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS gateway_properties (
                        key TEXT PRIMARY KEY,
                        reputation TEXT,
                        behavior_changed TEXT
                    )
                ''')
                
                # Inicializa a linha única com a chave fixa
                cursor.execute('''
                    INSERT OR IGNORE INTO gateway_properties (key, reputation, behavior_changed)
                    VALUES (?, '0.5', 'False')
                ''', (self._STATE_KEY,))
                
                conn.commit()

        except Exception as e:
            self.logger.error(f"Erro crítico ao inicializar a base de dados de estado: {e}")
    
    # ==========================================
    # GESTÃO DA REPUTAÇÃO (FLOAT)
    # ==========================================

    def set_reputation(self, reputation_value):
        """Atualiza a reputação na linha fixa."""

        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    INSERT INTO gateway_properties (key, reputation) 
                    VALUES (?, ?)
                    ON CONFLICT(key) 
                    DO UPDATE SET reputation = excluded.reputation
                ''', (self._STATE_KEY, str(float(reputation_value))))
                conn.commit()
        except Exception as e:
            self.logger.error(f"Erro ao salvar reputação: {e}")

    def get_reputation(self):
        """Coleta a reputação e retorna como Float."""

        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('SELECT reputation FROM gateway_properties WHERE key = ?', (self._STATE_KEY,))
                row = cursor.fetchone()
                return float(row[0]) if row and row[0] is not None else 0.5
        except Exception as e:
            self.logger.error(f"Erro ao ler reputação: {e}")
            return 0.5

    # ==========================================
    # GESTÃO DE MUDANÇA DE CONDUTA (BOOLEAN)
    # ==========================================

    def set_behavior_changed(self, changed_status):
        """Atualiza a flag de conduta na linha fixa."""
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                str_status = str(bool(changed_status))
                cursor.execute('''
                    INSERT INTO gateway_properties (key, behavior_changed) 
                    VALUES (?, ?)
                    ON CONFLICT(key) 
                    DO UPDATE SET behavior_changed = excluded.behavior_changed
                ''', (self._STATE_KEY, str_status))
                conn.commit()
        except Exception as e:
            self.logger.error(f"Erro ao salvar behavior_changed: {e}")

    def get_behavior_changed(self):
        """Coleta a flag e retorna como Boolean."""
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('SELECT behavior_changed FROM gateway_properties WHERE key = ?', (self._STATE_KEY,))
                row = cursor.fetchone()
                return row[0] == 'True' if row and row[0] is not None else False
        except Exception as e:
            self.logger.error(f"Erro ao ler behavior_changed: {e}")
            return False