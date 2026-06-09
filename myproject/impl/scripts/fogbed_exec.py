from fogledgerIota.iota.IotaBasic import (IotaBasic)
from fogledgerIota.iota.config.NodeConfig import (NodeConfig)
from fogledgerIota.iota.config.CoordConfig import (CoordConfig)
from fogledgerIota.iota.config.SpammerConfig import (SpammerConfig)
from fogledgerIota.iota.config.ApiConfig import (ApiConfig)
from fogledgerIota.iota.config.WebAppConfig import (WebAppConfig)
from typing import List
from fogbed import (
    VirtualInstance, setLogLevel, FogbedDistributedExperiment, Worker, Container, Controller
)
import signal
import time



def criar_tangle(exp: FogbedDistributedExperiment, worker_list: List) -> IotaBasic:

    node_list = []

    for i in range(len(worker_list)):
        node = NodeConfig(name=f'node{i}')
        node_list.append(node)
    
    cord = CoordConfig(name='cord', interval='60s')
    spammer = SpammerConfig(name='spammer', message ='one-click-tangle.')

    iota = IotaBasic(exp=exp, prefix='iota1', conf_nodes=node_list)

    for i in range(len(worker_list)):
        worker_list[i].add(iota.ledgers[i], reachable=True)

    worker_list[0].add(iota.ledgers[len(worker_list)], reachable=True)
    worker_list[0].add(iota.ledgers[len(worker_list)+1], reachable=True)

    return iota


def criar_tuneis(worker_list: List):

    for i in range(len(worker_list) - 1):
        exp.add_tunnel(worker_list[i], worker_list[i+1])

    return


def criar_instancias_virtuais(exp: FogbedDistributedExperiment, qtd: int) -> List:

    instVirt_list = []

    for i in range(qtd):
        instVirt = exp.add_virtual_instance(f'worker{i}')
        instVirt_list.append(instVirt)

    return instVirt_list


def criar_pontes_zmq(exp: FogbedDistributedExperiment, worker_list: List, instVirt_list: List, iota: IotaBasic) -> List:

    zmq_list = []

    for i in range(len(worker_list)):

        zmq = Container(
            name=f'zmq{i}',
            dimage='silviozv/iota-zmq-bridge:1.0.0',
            dcmd=f'/entrypoint.sh',
            environment={
                'MQTT_IP': iota.containers[f'node{i}'].ip,
                'INDEXES': 'REP_*'
            }
        )
        exp.add_docker(zmq, instVirt_list[i])

        print("Ponte ZMQ: ", zmq.ip)

        zmq_list.append(zmq)
    
    return zmq_list


def criar_apis_tangle(exp: FogbedDistributedExperiment, worker_list: List, instVirt_list: List, iota: IotaBasic) -> List:

    api_list = []

    for i in range(len(worker_list)):

        api = Container(
            name=f'api{i}',
            dimage='silviozv/tangle-hornet-api:1.0.0',
            dcmd='./entrypoint.sh', 
            environment={
                'API_PORT': '3000',
                'TANGLE_NODE_URL': str(iota.containers[f'node{i}'].ip),
                'TANGLE_NODE_PORT': '14265'
            }
        )
        
        exp.add_docker(api, instVirt_list[i])

        print(f"API Tangle: {api.ip}")

        api_list.append(api)
    
    return api_list


def definir_gateway_tipo(quant_honestos: int, quant_maliciosos: int, quant_egoistas: int, quant_perturbadores: int) -> List:

    gateway_tipo_list = []

    for i in range(quant_honestos):
        gateway_tipo_list.append('1')

    for i in range(quant_maliciosos):
        gateway_tipo_list.append('2')

    for i in range(quant_egoistas):
        gateway_tipo_list.append('3')

    for i in range(quant_perturbadores):
        gateway_tipo_list.append('4')
    
    print("Lista tipos", gateway_tipo_list)

    return gateway_tipo_list


def definir_qtd_gateways_por_worker(qtd_workers: int, qtd_gateways: int) -> List:

    qtd_gateways_por_worker_list = []

    for i in range(qtd_workers):
        qtd_gateways_por_worker_list.append(0)
    
    j = 0
    for i in range(qtd_gateways):
        qtd_gateways_por_worker_list[j] += 1

        if j == (qtd_workers-1):
            j = 0
        else:
            j += 1
    
    print("Quantidade gateways por worker: ", qtd_gateways_por_worker_list)

    return qtd_gateways_por_worker_list


def link_instVirt_worker(instVirt_list: List, worker_list: List) -> List:

    for i in range(len(worker_list)):
        worker_list[i].add(instVirt_list[i], reachable=True)

    return


########### NOVO ###############

import os

# Definição de caminhos globais (coloque no início do seu arquivo principal)
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(CURRENT_DIR, '../../'))

def criar_gateways(exp, iota, worker_list, instVirt_list, qtd_gateways_por_worker_list, zmq_list, api_list, gateway_tipo_list, honestidade_malicioso, honestidade_perturbador):

    # 1. GERAÇÃO DO ARQUIVO ENV.INI (Feito apenas uma vez antes de subir os nós)
    os.makedirs('config/auto-generated', exist_ok=True)
    with open('config/auto-generated/env.ini', 'w') as f:
        f.write("[env]\n")
        f.write("My_API_Password_1=senha123\n") 
        f.write("My_API_Password_2=senha123\n")
        f.write("Zato_Project_Root=/opt/hot-deploy/myproject\n")

    count_ip = (2 + (3 * len(worker_list))) + 1
    gateways_list = []
    z = 0

    for i in range(len(worker_list)):
        for j in range(qtd_gateways_por_worker_list[i]):

            ip_gateway = f'10.0.0.{count_ip}'

            if gateway_tipo_list[z] == '1':
                taxa_de_honestidade = '100'
            elif gateway_tipo_list[z] == '2':
                taxa_de_honestidade = str(honestidade_malicioso)
            elif gateway_tipo_list[z] == '3':
                taxa_de_honestidade = '100'
            elif gateway_tipo_list[z] == '4':
                taxa_de_honestidade = str(honestidade_perturbador)

            # Instanciação do Container
            gat = Container(
                    name=f'gat{((i+1)*10) + j}',
                    ip=ip_gateway,
                    user='root',
                    privileged=True,
                    dimage='rhianpablo11/esb-zato-soft-iot:v11',
                    dcmd='bash -c "sleep 60 && /usr/local/bin/start_wrapper.sh"',
                    environment={
                        'Zato_Dashboard_Password': '123456',
                        'ZATO_SSH_PASSWORD': '123456',
                        'Zato_IDE_Password': '123456',
                        'Zato_Log_Env_Details': 'true',
                        'Zato_SAVE_DATA_ENABLED': 'True',
                        'Zato_COLLECTION_TIME': '2',
                        'Zato_PUBLISH_TIME': '6',
                        'Zato_AGGREGATION_WINDOW_MINUTES': '10',
                        'Zato_DATA_RETENTION_SECONDS': '1200',
                        'Zato_TANGLE_API_IP': str(api_list[i].ip), 
                        'Zato_TANGLE_API_PORT': '3000',
                        'Zato_ZMQ_IP': str(zmq_list[i].ip),
                        'Zato_ZMQ_PORT': '5556',
                        'Zato_NODE_TYPE': gateway_tipo_list[z], 
                        'Zato_HONESTY_RATE': taxa_de_honestidade,
                        'Zato_GROUP': 'cloud/c1',
                        'Zato_GATEWAY_REAL_IP': ip_gateway
                    },
                    port_bindings={
                        11223: f""
                    }
                )

            gateways_list.append(gat)
            exp.add_docker(gat, instVirt_list[i])

            z += 1
            count_ip += 1

            print(f"Gateway: {gat.ip} | Honestidade: {taxa_de_honestidade} | Porta: {11223 + (i+j)}")
        
    return gateways_list


def configurar_gateways_pos_start(gateways_list):
    """
    Injeta o código Python e as configurações nos containers do Zato
    apenas após a rede Fogbed estar em execução.
    """
    print("\n[INFO] Iniciando injeção de arquivos nos Gateways...")

    for gat in gateways_list:
        real_docker_name = f"mn.{gat.name}"
        
        print(f"Configurando diretórios no container {real_docker_name}...")
        gat.cmd('mkdir -p /opt/hot-deploy/myproject /opt/hot-deploy/enmasse /opt/hot-deploy/python-reqs /home/ubuntu/mapping_archives/devices_config/')

        print(f"Copiando arquivos do Host ({PROJECT_ROOT}) para o container {real_docker_name}...")
        os.system(f"docker cp {PROJECT_ROOT}/. {real_docker_name}:/opt/hot-deploy/myproject/")
        os.system(f"docker cp {PROJECT_ROOT}/config/enmasse/enmasse.yaml {real_docker_name}:/opt/hot-deploy/enmasse/enmasse.yaml")
        os.system(f"docker cp {PROJECT_ROOT}/config/auto-generated/env.ini {real_docker_name}:/opt/hot-deploy/enmasse/env.ini")
        os.system(f"docker cp {PROJECT_ROOT}/config/python-reqs/requirements.txt {real_docker_name}:/opt/hot-deploy/python-reqs/requirements.txt")
        os.system(f"docker cp {PROJECT_ROOT}/impl/src/archives/. {real_docker_name}:/home/ubuntu/mapping_archives/devices_config/")
        
        # Limpeza
        os.system(f"docker exec {real_docker_name} rm -f /opt/hot-deploy/myproject/impl/scripts/fogbed-test.py")
        
        print(f"✅ Container {gat.name} configurado com sucesso!")


def criar_devices(exp: FogbedDistributedExperiment, qtd_devices_por_gateway: int, qtd_gateways_por_worker_list: List, gateways_list: List, worker_list: List, instVirt_list: List) -> List:

    # Cálculo seguro do IP base para não colidir
    count_ip = (2 + (4 * len(worker_list)) + len(gateways_list)) + 1

    devices_list = []
    
    # Índice para rastrear qual gateway pertence à iteração atual
    count_gateway = 0 

    for i in range(len(worker_list)):

        for j in range(qtd_gateways_por_worker_list[i]):
            
            # Pega o objeto do gateway atual para extrair o IP dele
            gateway_atual = gateways_list[count_gateway]

            for z in range(qtd_devices_por_gateway):

                device_nome = f'dev{i}_{j}_{z}'
                interface_nome = f'{device_nome}-eth0' 
                ip_device = f'10.0.0.{count_ip}'
                count_ip += 1

                # 1. Comando para instalar o pacote de redes
                install_cmd = "apt-get update && apt-get install -y iproute2"

                # 2. Ativação da interface e criação da rota manualmente no Fogbed
                net_setup = f"ip link set dev {interface_nome} up && ip addr add {ip_device}/8 dev {interface_nome} && ip route add 10.0.0.0/8 dev {interface_nome} || true"

                # 3. Pipeline de execução completo:
                # - Instala rede -> Espera 5s -> Configura IP/Rota -> Espera 60s (Zato Boot) -> Roda o App Python
                # - O "; tail -f /dev/null" no final garante que o container não morra se o Python falhar, permitindo debug.
                dcmd_completo = f'bash -c "{install_cmd} && sleep 5 && {net_setup} && sleep 120; python -m app.main; tail -f /dev/null"'

                dev = Container(
                    name=device_nome,
                    dimage='silviozv/python-iot-device:1.0.0',
                    ip=ip_device,
                    dcmd=dcmd_completo,
                    environment={
                        'BROKER_IP': str(gateway_atual.ip),
                        'BROKER_PORT': '1883'
                    }
                )

                devices_list.append(dev)
                exp.add_docker(dev, instVirt_list[i])
            
                print(f"Device: {dev.ip} (Interface: {interface_nome}) -> Conectado ao Gateway: {gateway_atual.ip}")

            count_gateway += 1

    return devices_list


########### NOVO ###############


# def criar_gateways(exp: FogbedDistributedExperiment, iota: IotaBasic, worker_list: List, instVirt_list: List, qtd_gateways_por_worker_list: List, zmq_list: List, api_list: List, gateway_tipo_list: List, honestidade_malicioso: int, honestidade_perturbador: int) -> List:

#     count_ip = (2 + (3 * len(worker_list))) + 1

#     gateways_list = []

#     z = 0

#     for i in range(len(worker_list)):

#         for j in range(qtd_gateways_por_worker_list[i]):

#             ip_gateway = f'10.0.0.{count_ip}'
#             count_ip += 1

#             if gateway_tipo_list[z] == '1':
#                 taxa_de_honestidade = '100'
#             elif gateway_tipo_list[z] == '2':
#                 taxa_de_honestidade = str(honestidade_malicioso)
#             elif gateway_tipo_list[z] == '3':
#                 taxa_de_honestidade = '100'
#             elif gateway_tipo_list[z] == '4':
#                 taxa_de_honestidade = str(honestidade_perturbador)

#             '''gat = Container(
#                 name=f'gat{((i+1)*10) + j}',
#                 dimage='silviozv/reputation-system:1.4.4', 
#                 ip=ip_gateway,
#                 dcmd='bash -c "sleep 60 && /bin/bash /usr/local/bin/karaf-init.sh"',
#                 environment={ 
#                     'COLLECT_TIME':'2000',
#                     'PUBLISH_TIME': '200',
#                     'TANGLE_NODE_URL':iota.containers[f'node{i}'].ip,
#                     'ZMQ_SOCKET_PROTOCOL':'tcp',
#                     'ZMQ_SOCKET_URL':str(zmq_list[i].ip),
#                     'ZMQ_SOCKET_PORT': '5556',
#                     'NODE_TYPE': gateway_tipo_list[z],
#                     'HONESTY_RATE': taxa_de_honestidade,
#                     'CHECK_DEVICE':'5',
#                     'REQUEST_DATA':'10',
#                     'WAIT_DEVICE_RESPONSE':'3',
#                     'CHECK_NODES_SERVICE':'20',
#                     'WAIT_NODES_RESPONSE':'15',
#                     'USE_CREDIBILITY':'true',
#                     'USE_LATEST_CREDIBILITY':'true',
#                     'GATEWAY_REAL_IP': ip_gateway
#                 }
#             )'''

#             gat = Container(
#                     name=f'gat{((i+1)*10) + j}',
#                     ip=ip_gateway,
#                     user='root',
#                     privileged=True,
#                     dimage='rhianpablo11/esb-zato-soft-iot:v11',
#                     dcmd='/usr/local/bin/start_wrapper.sh',    
#                     environment={
#                         'Zato_Dashboard_Password': '123456',
#                         'ZATO_SSH_PASSWORD': '123456',
#                         'Zato_IDE_Password': '123456',
#                         'Zato_Log_Env_Details': 'true',
#                         'Zato_Build_Verbosity': '',
#                         'Zato_SAVE_DATA_ENABLED': 'True',
#                         'Zato_COLLECTION_TIME': '2',
#                         'Zato_PUBLISH_TIME': '6',
#                         'Zato_AGGREGATION_WINDOW_MINUTES': '10',
#                         'Zato_DATA_RETENTION_SECONDS': '1200',
#                         'Zato_TANGLE_API_IP': str(api_list[i].ip), 
#                         'Zato_TANGLE_API_PORT': '3000',
#                         'Zato_ZMQ_IP': str(zmq_list[i].ip),
#                         'Zato_ZMQ_PORT': '5556',
#                         'Zato_NODE_TYPE': gateway_tipo_list[z], 
#                         'Zato_HONESTY_RATE': taxa_de_honestidade,
#                         'Zato_GROUP': 'cloud/c1',
#                         'Zato_GATEWAY_REAL_IP': ip_gateway
#                     },
#                     port_bindings={
#                         11223: f"1122{count_ip}"
#                     }
#                 )

#             gateways_list.append(gat)

#             exp.add_docker(gat, instVirt_list[i])

#             z += 1

#             print("Gateway: ", gat.ip)
#             print("Honestidade: ", taxa_de_honestidade)
        
#     return gateways_list


'''
def criar_devices(exp: FogbedDistributedExperiment, qtd_devices_por_gateway: int, qtd_gateways_por_worker_list: List, gateways_list: List, worker_list: List, instVirt_list: List) -> List:

    count_ip = (2 + (2 * len(worker_list)) + len(gateways_list)) + 1

    devices_list = []

    count = 0

    for i in range(len(worker_list)):

        for j in range(qtd_gateways_por_worker_list[i]):

            for z in range(qtd_devices_por_gateway):

                device_nome = f'dev{i}_{j}_{z}'

                interface_nome = f'{device_nome}-eth0' 

                ip_device = f'10.0.0.{count_ip}'
                count_ip += 1

                install_cmd = "apt-get update && apt-get install -y iproute2"

                net_setup = f"ip link set dev {interface_nome} up && ip addr add {ip_device}/8 dev {interface_nome} && ip route add 10.0.0.0/8 dev {interface_nome} || true"

                dcmd_completo = f'bash -c "{install_cmd} && sleep 5 && {net_setup} && sleep 110 && java -jar device.jar -bi {gateways_list[count].ip} -cd 1"'

                dev = Container(
                    name=device_nome,
                    dimage='silviozv/python-iot-device:1.0.0',
                    ip=ip_device,
                    dcmd=dcmd_completo
                )

                devices_list.append(dev)

                exp.add_docker(dev, instVirt_list[i])
            
                print("Device: ", dev.ip)

            count += 1

    return devices_list
'''


setLogLevel('info')

if (__name__ == '__main__'):


    exp = FogbedDistributedExperiment()

    worker_list = []

    # worker_list.append(exp.add_worker('larsid01'))
    # worker_list.append(exp.add_worker('larsid02'))
    # worker_list.append(exp.add_worker('larsid03'))
    # worker_list.append(exp.add_worker('larsid04'))
    # worker_list.append(exp.add_worker('larsid05'))
    # worker_list.append(exp.add_worker('larsid06'))
    # worker_list.append(exp.add_worker('larsid07'))
    # worker_list.append(exp.add_worker('larsid08'))
    # worker_list.append(exp.add_worker('larsid09'))
    # worker_list.append(exp.add_worker('larsid10'))
    # worker_list.append(exp.add_worker('larsid11'))
    # worker_list.append(exp.add_worker('larsid12'))
    # worker_list.append(exp.add_worker('larsid13'))
    # worker_list.append(exp.add_worker('larsid14'))
    worker_list.append(exp.add_worker('larsid15'))
    # worker_list.append(exp.add_worker('larsid16'))


    #quant_honestos = int(input("\nQuantidade de honestos: "))
    #quant_maliciosos = int(input("Quantidade de maliciosos: "))
    #quant_egoistas = int(input("Quantidade de egoístas: "))
    #quant_perturbadores = int(input("Quantidade de perturbadores: "))

    #honestidade_malicioso = int(input("Taxa de honestidade do nó malicioso (0-100): "))
    #honestidade_perturbador = int(input("Taxa de honestidade do nó perturbador (0-100): "))

    #quant_devices = int(input("\nQuantidade de devices por gateway: "))

    quant_honestos = 1
    quant_maliciosos = 1
    quant_egoistas = 0
    quant_perturbadores = 0

    honestidade_malicioso = 20
    honestidade_perturbador = 0

    quant_devices = 1

    
    qtd_gateways = quant_honestos + quant_maliciosos + quant_egoistas + quant_perturbadores
    qtd_workers = len(worker_list)


    if qtd_gateways < qtd_workers:
       print("\nQuantidade de gateways menor que quantidade de workers")
    
    else:

        iota = criar_tangle(exp, worker_list)

        instVirt_list = criar_instancias_virtuais(exp, qtd_workers)

        zmq_list = criar_pontes_zmq(exp, worker_list, instVirt_list, iota)

        api_list = criar_apis_tangle(exp, worker_list, instVirt_list, iota)

        gateway_tipo_list = definir_gateway_tipo(quant_honestos, quant_maliciosos, quant_egoistas, quant_perturbadores)

        qtd_gateways_por_worker_list = definir_qtd_gateways_por_worker(qtd_workers, qtd_gateways)

        link_instVirt_worker(instVirt_list, worker_list)

        gateways_list = criar_gateways(exp, iota, worker_list, instVirt_list, qtd_gateways_por_worker_list, zmq_list, api_list, gateway_tipo_list, honestidade_malicioso, honestidade_perturbador)

        devices_list = criar_devices(exp, quant_devices, qtd_gateways_por_worker_list, gateways_list, worker_list, instVirt_list)

        criar_tuneis(worker_list)

        try:
            exp.start()
            iota.start_network()
            print("Experimento iniciado")

            configurar_gateways_pos_start(gateways_list)

            while True:

                key = input("\nPressione 'p' para parar o experimento: ")
                if key.lower() == 'p':
                    print("Parando experimento...")
                    exp.stop()
                    break
                else:
                    print(f"Entrada ignorada: {key}")

        except Exception as ex:
            print(ex)
        finally:
            exp.stop()
