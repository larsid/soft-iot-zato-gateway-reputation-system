# Soft-IoT Local Storage (Python Version)

Este projeto consiste na portabilidade e evolução do sistema **Soft-IoT Local Storage** e **Soft-IoT Mapping Devices** da linguagem Java para **Python**. A solução foi reestruturada utilizando a arquitetura **Zato Project Blueprint**, visando uma implantação moderna baseada em microsserviços e práticas de DevOps.

O projeto atua como um **Gateway e Armazenamento Local** para dispositivos IoT, orquestrando a comunicação via protocolo TATU, gerenciando um Broker MQTT interno e expondo APIs de serviço via Zato ESB.

## 📂 Estrutura do Projeto

O projeto segue a estrutura de diretórios do [Zato Blueprint](https://zato.io/en/tutorials/devops/deployment.html), separando configurações, dependências e implementação:

```text
myproject/
├── config/
│   ├── enmasse/          # Definições de canais, segurança e conexões (YAML)
│   ├── python-reqs/      # Dependências Python (pip)
│   └── user-conf/        # Configurações .ini do Zato
└── impl/
    ├── scripts/          # Scripts de provisionamento (run-container.sh)
    └── src/
        └── services/          # Serviços Zato (Lógica de negócio em Python)
        └── archives/          # Arquivos de configuração dos dispositivos IoT
```

-----

## 🚀 Componentes e Módulos

O funcionamento do sistema depende de uma **Imagem Docker Personalizada** que integra múltiplos serviços em um único container. Abaixo estão os principais módulos que compõem a solução:

### 1\. Imagem Personalizada do Zato (`esb-zato-soft-iot`)

Diferente de uma instalação padrão, este projeto utiliza uma imagem Docker customizada (`esb-zato-soft-iot`) que adiciona funcionalidades críticas para o ambiente IoT:

  * **Broker MQTT (Mosquitto):** Integrado diretamente no container. O Zato e os dispositivos comunicam-se localmente ou externamente através das portas `1883` (TCP) e `9001` (WebSockets).
  * **Extended TATU Wrapper:** A biblioteca Python (`extended-tatu-wrapper`) é instalada nativamente na imagem, permitindo que os serviços do Zato compreendam e construam mensagens do protocolo TATU (FLOW, GET, CONNECT).
  * **Orquestração de Inicialização (`start_wrapper.sh`):** Um script bash que garante a ordem correta de subida dos serviços:
    1.  Inicia o Mosquitto em background.
    2.  Aguarda a liberação das portas.
    3.  Inicia o Scheduler Externo.
    4.  Transfere o controle para o processo principal do Zato.

### 2\. Scheduler Customizado (`custom_scheduler.py`)

Um agendador externo desenvolvido em Python que roda paralelamente ao Zato.

  * **Função:** Ler o arquivo de configuração `enmasse.yaml` e disparar requisições HTTP para os serviços do Zato (Jobs) conforme configurado.
  * **Diferencial:** Possui uma lógica de `wait_for_zato`, garantindo que os jobs só comecem a ser disparados quando a API do Zato estiver respondendo ao `/ping` (status 200).

### 3\. Protocolo TATU (Python)

A lógica de comunicação IoT foi migrada do Java para Python, utilizando o repositório `extended-tatu-wrapper`. Isso permite que o Local Storage interprete payloads JSON complexos enviados pelos dispositivos virtuais (Virtual FoT Device).

-----

## 🛠️ Como Executar

O projeto é totalmente containerizado. Para iniciar o ambiente:

### Pré-requisitos

  * Docker instalado e rodando.
  * Portas `1883`, `8183`, `8184`, `11225`, `33033`, `35672`, `9001`, `11223`, `22022`,  livres no host.

### Passo a Passo

1.  **Navegue até o script de execução:**

    ```bash
    cd myproject/impl/scripts
    ```

2.  **Execute o container:**
    O script `run-container.sh` foi configurado para montar os volumes de código (`src/`) e configuração (`enmasse.yaml`) automaticamente.

    ```bash
    ./run-container.sh
    ```

    *Este comando irá parar qualquer container anterior com o mesmo nome, fazer o pull da imagem base (se necessário) e iniciar o ambiente.*


    *Você deve ver mensagens como `[WRAPPER] Iniciando Mosquitto Broker...` e `Zato está ONLINE! Iniciando agendamento...`.*

-----

## 🧪 Como Testar

Após a inicialização, você pode testar o sistema com um dispositivo virtual:


### 1\. Integração com Dispositivos

Para testar o fluxo completo, execute a versão Python do **Virtual FoT Device** (presente no repositório [`virtual-fot-device:python_version`](https://github.com/larsid/virtual-fot-device/tree/python_version)).

1.  Configure o dispositivo para apontar para o IP da sua máquina (onde o container Zato está rodando).
2.  O dispositivo enviará um `CONNECT` para o tópico `dev/CONNECTIONS`.
3.  O Local Storage (Zato) deve processar e responder o dispositivo.
4.  Caso o dispositivo tenha uma configuração pre-existente ela será aplicada para o controle do dispositivo, caso contrário será cadastrado o dispositivo e aplicada uma configuração aleatoria dentre os padrões pre-existentes.

-----

## 📚 Repositórios Base e Referências

Este projeto é o resultado da integração e conversão dos seguintes repositórios:

1.  **[Virtual FoT Device (Java/Python)](https://www.google.com/search?q=https://github.com/larsid/virtual-fot-device):**
      * Origem dos dispositivos simulados. A versão Python deste dispositivo é utilizada para enviar dados a este Local Storage.
2.  **[Extended TATU Wrapper](https://www.google.com/search?q=https://github.com/larsid/extended-tatu-wrapper):**
      * Biblioteca fundamental portada para Python que permite a serialização/deserialização das mensagens IoT.
3.  **[Zato Project Blueprint](https://github.com/zatosource/zato-project-blueprint):**
      * Base arquitetural utilizada para organizar este repositório, facilitando a gestão de configuração (Enmasse) e deploy via Docker.
4.  **[Zato ESB Services (Imagem Customizada)](https://hub.docker.com/r/rhianpablo11/esb-zato-soft-iot):**
      * Contém os scripts `Dockerfile`, `start_wrapper.sh` e `custom_scheduler.py` que permitem rodar o Zato junto com o Mosquitto.

-----

## 📝 Notas de Desenvolvimento

  * **Migração Java -\> Python:** A lógica de persistência e tratamento de mensagens que residia em classes Java (Controllers/Models) agora deve ser implementada como **Serviços Zato** dentro de `impl/src/services/`.
  * **Enmasse:** Toda a configuração de canais REST, segurança e agendamentos deve ser feita no arquivo `config/enmasse/enmasse.yaml`, que é carregado automaticamente no boot.