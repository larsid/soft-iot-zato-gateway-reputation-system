# -*- coding: utf-8 -*-

import os
import math
import json
import logging
from zato.server.service import Service

logger = logging.getLogger('zato.reputation')

class ReputationOrchestrator(Service):
    """
    Serviço principal que coordena o cálculo de reputação de um nó.
    """
    name = 'soft-iot.reputation.orchestrator'

    def handle(self):
        target_node_id = self.request.payload.get('node_id')
        
        if not target_node_id:
            self.response.payload = {"status": "error", "message": "node_id é obrigatório"}
            return

        # 1. Busca avaliações históricas na Tangle para este nó
        self.logger.info(f"Buscando avaliações na Tangle para o nó: {target_node_id}")
        tangle_res = self.invoke('soft-iot.dlt.client.api.read_index', {'index': target_node_id})
        
        valid_evaluations = []
        
        # Extração segura dos dados, considerando possíveis strings escapadas
        if isinstance(tangle_res, list):
            for tx in tangle_res:
                try:
                    content = tx.get('data', {})
                    if isinstance(content, str):
                        try:
                            content = json.loads(content)
                        except json.JSONDecodeError:
                            continue
                    
                    # Adiciona transações de avaliação (REP_EVALUATION)
                    if content.get('type') == 'REP_EVALUATION':
                        valid_evaluations.append({
                            'source': content.get('source'),
                            'value': float(content.get('value', 0.0)),
                            'credibility': float(content.get('nodeCredibility', 0.0))
                        })
                except (ValueError, TypeError, AttributeError):
                    continue

        # Se não há dados, retorna a reputação inicial (0.5)
        if len(valid_evaluations) == 0:
            self.logger.warning(f"Sem dados na Tangle para {target_node_id}. Retornando 0.5 (neutro).")
            self.response.payload = {"node_id": target_node_id, "reputation": 0.5, "status": "no_data"}
            return

        # 2. Executa o K-Means sobre as credibilidades
        credibilities = [eval_data['credibility'] for eval_data in valid_evaluations]
        
        self.logger.info(f"Executando K-Means sobre as credibilidades de {len(credibilities)} avaliadores")
        kmeans_res = self.invoke('soft-iot.kmeans.credibility', {'data': credibilities})
        
        # Maiores credibilidades
        trusted_credibilities = kmeans_res if isinstance(kmeans_res, list) else []

        if not trusted_credibilities:
            self.logger.error("Falha no K-Means, utilizando todos os avaliadores como fallback.")
            trusted_credibilities = credibilities

        # 3. Filtra as avaliações: mantemos apenas aquelas cuja credibilidade está no cluster seleto
        trusted_evaluations = []
        for ev in valid_evaluations:
            for tc in trusted_credibilities:
                # Uso de math.isclose para evitar problemas de precisão com floats
                if math.isclose(ev['credibility'], tc, rel_tol=1e-5):
                    trusted_evaluations.append(ev)
                    break

        if not trusted_evaluations:
            trusted_evaluations = valid_evaluations  # Fallback de segurança

        # 4. Calcula a Reputação Final: Média aritmética dos 'values' do cluster confiável
        soma_valores = sum(ev['value'] for ev in trusted_evaluations)
        final_reputation = soma_valores / len(trusted_evaluations)

        # Garante o limite estrito do intervalo [-1.0, 1.0]
        final_reputation = max(-1.0, min(1.0, final_reputation))

        # Retorna o valor sob demanda (a escrita desnecessária de consenso na Tangle foi removida)
        self.response.payload = {
            "node_id": target_node_id,
            "reputation": round(final_reputation, 4),
            "evaluations_used": len(trusted_evaluations),
            "status": "success"
        }


class CredibilityManager(Service):
    """
    Gerencia a credibilidade de quem avalia.
    """
    name = 'soft-iot.reputation.credibility.manager'

    def handle(self):
        payload = self.request.payload
        evaluator_id = payload.get('evaluator_id')
        
        current_cred = float(payload.get('current_credibility', 0.5))
        evaluation_given = float(payload.get('evaluation_given', 0.0)) # Nota atual
        last_evaluation_given = float(payload.get('last_evaluation_given', evaluation_given)) # Nota anterior do MESMO avaliador
        consensus_reputation = float(payload.get('consensus_reputation', 0.0)) # Verdade da rede

        # Confiabilidade: Comparação com a verdade da rede (Consenso)
        reliability = 1.0 - abs(consensus_reputation - evaluation_given)
        
        # Consistência: Comparação com a própria nota anterior (Histórico do Avaliador)
        # Conforme implementado em NodeCredibility.java
        consistency = 1.0 - abs(evaluation_given - last_evaluation_given)

        # Thresholds (Limiares) definidos no .cfg do Java
        RELIABILITY_THRESHOLD = 0.75
        CONSISTENCY_THRESHOLD = 0.75

        new_cred = current_cred

        # Lógica de Bonificação/Penalidade fiel ao Java
        if reliability >= RELIABILITY_THRESHOLD and consistency >= CONSISTENCY_THRESHOLD:
            new_cred += 0.10
        elif reliability >= RELIABILITY_THRESHOLD:
            new_cred += 0.05
        elif reliability < RELIABILITY_THRESHOLD and consistency >= CONSISTENCY_THRESHOLD:
            new_cred -= 0.10
        else:
            new_cred -= 0.05

        new_cred = max(-1.0, min(1.0, new_cred))

        self.response.payload = {
            "evaluator_id": evaluator_id,
            "new_credibility": round(new_cred, 4),
            "reliability": round(reliability, 4),
            "consistency": round(consistency, 4)
        }