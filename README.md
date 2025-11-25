# Robust Web Automation Data Pipeline
Automação robusta para extração estruturada de dados em um portal financeiro externo, utilizando Python e Selenium.  
Esta é uma versão pública e totalmente anonimizada, preservando a integridade do ambiente real e mantendo o foco na qualidade técnica da automação.

---

## 1. Visão Geral
Este projeto implementa um sistema completo de automação para coleta de dados em um portal financeiro com interface dinâmica, múltiplos fluxos de validação e comportamento assíncrono.

A solução oferece:
- Navegação automatizada com Selenium
- Estratégias de tolerância a falhas
- Mapeamento dinâmico de seletores via JSON
- Pipeline de entrada e saída em Excel
- Monitoramento em tempo quase real por meio de arquivo de streaming
- Logs estruturados
- Arquitetura modular baseada em boas práticas

A versão aqui publicada utiliza:
- URLs fictícias
- Seletores genéricos
- Dados de demonstração
- Ausência total de informações sensíveis ou empresariais

---

## 2. Arquitetura da Solução
A automação segue uma arquitetura estruturada:

1. Entrada de dados a partir de planilha Excel (lista de CPFs)  
2. Login automatizado no portal (ambiente genérico)  
3. Navegação até o módulo de consulta  
4. Preenchimento dos dados e disparo da consulta  
5. Esperas explícitas e sincronia de elementos dinâmicos  
6. Extração de informações estruturadas em memória  
7. Escrita incremental em arquivo de streaming para acompanhamento da execução  
8. Tratamento de erros, exceções e reprocessamento  
9. Consolidação final dos resultados em planilha Excel

A lógica de extração utiliza:
- Controle de fluxo orientado a registro
- WebDriverWait e Expected Conditions
- Múltiplos pontos de fallback de seletores
- Leitura de tabelas dinâmicas
- Estrutura padronizada de dados para consumo analítico

---

### 3. Streaming de dados durante a extração

Durante o processamento, a automação grava cada resultado parcial em um arquivo de streaming, permitindo acompanhar o progresso em tempo real.

O formato de streaming pode ser:

- `saida_streaming.txt` (texto separado por ponto e vírgula)
- `saida_streaming.xlsx` (caso configurado no .env)

Por padrão, esta versão utiliza o formato `.txt`, que é mais leve e mais adequado para acompanhamento em tempo real.

---

## 4. Tecnologias Utilizadas
- Python 3.11+
- Selenium WebDriver
- Microsoft Edge (Chromium) WebDriver
- Pandas
- Estrutura de logs (logging)
- JSON para mapeamento de seletores
- Arquitetura modular

---

## 5. Estrutura de Pastas
```text
project/
│
├── extrator_portal.py
├── selectors_example.json
├── .env.example
├── requirements.txt
├── .gitignore
├── README.md
│
├── dados/
│   ├── inserir_dados.xlsx
│   ├── saida_streaming.xlsx
│   ├── streaming_exemplo.txt
│
└── logs/              # criado automaticamente em tempo de execução (não versionado)

```

## 6. Configuração e Execução

### 6.1 Instalação

Instale as dependências necessárias:

```bash
pip install -r requirements.txt
```

### 6.2 Configuração das variáveis de ambiente

Crie um arquivo .env baseado em .env.example:

```text

PORTAL_URL=https://portal-financeiro-confidencial.com/login
DRIVER_PATH=C:\Drivers\msedgedriver.exe
SELECTORS_FILE=selectors_example.json
STREAMING_OUTPUT_PATH=./dados/saida_streaming.txt
```

### 6.3 Seletores

O arquivo selectors_example.json contém identificadores genéricos do portal.
Exemplo:

.json

Copiar código
{
  "campo_usuario": "usuarioInput",
  "campo_senha": "senhaInput",
  "botao_entrar": "btnLogin",
  "menu_cadastro": "menuCadastro",
  "menu_proposta": "menuConsultaProposta",
  "campo_cpf": "inputCPF",
  "grid_resultados": "tabelaResultados"
}


## 7. Execução da Automação

### 7.1 Via interface gráfica

```bash

python extrator_portal.py
```

## 7.2 Via execução direta (modo script)

```bash
python extrator_portal.py
```

-> Durante a execução, o sistema irá:

-> Carregar o arquivo de entrada

-> Processar cada CPF

-> Atualizar continuamente o arquivo de streaming configurado

-> Registrar logs detalhados por etapa

-> Consolidar ao final um arquivo de saída com todos os resultados

---
## 8. Exemplo de Entrada, Saída e Streaming

Entrada (inserir_dados.xlsx)

Nome	CPF	Matricula	Data_Extracao	Hora_Extracao	dt_nasc	renda																			
Ana Souza	00000000191	10234	2025-02-01	08:10	1978-03-20	2500																			

Streaming (streaming_exemplo.txt)
Arquivo atualizado ao longo da execução, contendo linhas já processadas, status e eventuais mensagens de controle.

Saída (saida_streaming.xlsx)

CPF	nome	DDDres	Tel_res	DDD_cel	Celular	Matricula	ttl_Parc_Pg	Parc_venc	ParcmAtraso_Parc_Pagas	Taxa	VlrParc	sld_ttal_Data
00000000191	Ana Souza	61	33445566	61	991234567	10234	12	0	0	1.80	255.90	2025-02-01
50000000285	erro	-	-

## 9. Evolução Futura e Migração para AWS

A estrutura atual foi concebida para rodar em ambiente local ou em servidores tradicionais, mas já considera uma futura migração para nuvem.

Evoluções planejadas:

Armazenamento dos arquivos de entrada, streaming e saída em Amazon S3

Orquestração do fluxo de extração utilizando AWS Step Functions ou AWS Lambda para partes específicas do processo

Utilização de AWS CloudWatch para monitoramento centralizado de logs e métricas

Integração com serviços de processamento de dados (por exemplo, AWS Glue ou EMR), conforme aumento de volume e demanda

A proposta é que, à medida que o volume de dados e a criticidade da automação cresçam, o pipeline evolua de um modelo local para uma arquitetura em nuvem, mais escalável, observável e resiliente.

## 10. Aviso de Segurança e Conformidade

Esta versão é exclusivamente demonstrativa e não contém:

URLs reais

Seletores reais

Dados reais

Credenciais

Informações do cliente ou do portal

O objetivo é apresentar a estrutura técnica da automação e demonstrar capacidade de engenharia de dados aplicada.

## 11. Licença e Autoria
Projeto desenvolvido por Marcos Soares.
Uso permitido para fins educacionais, demonstrações técnicas e portfólio profissional.
