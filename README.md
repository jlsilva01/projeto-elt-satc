# Projeto para Extração de dados de um SQL Server e Carga em um Data Lake Storage da Azure (ADLS) + Airflow 3 Astro CLI 

![image](https://github.com/user-attachments/assets/c2bdc91f-6dad-4534-b919-4c1dd5df4736)


## Container docker do SQL Server através do docker compose

### 1. Criar/iniciar um container docker do SQL Server, digitar o comando abaixo:

```
cd docker/sqlserver/
docker compose up -d
```
![image](https://github.com/user-attachments/assets/7a1fb296-ae4b-4697-87d1-1510b946b2d4)

Assim que o banco de dados estiver no ar, atualize os parâmetros do SQL no arquivo `.env`.

### 2. Parar o container do SQL Server, digitar o comando abaixo:

```
cd docker/sqlserver/
docker compose down
```
![image](https://github.com/user-attachments/assets/6966299f-3a94-4079-a1e6-21dbc43d2358)

## Criação do Azure DataLake Storage

Veja o [README](/iac/adls/README.md) da pasta `/iac/adls`.


## Execução dos Scripts Python para o ELT

Este projeto utiliza a versão `3.12` do Python, o gerenciador de projeto em Python `uv` (Universal Versioning) e as seguintes bibliotecas (estão no `pyproject.toml`):

```
    "azure-core",
    "azure-identity",
    "azure-storage-file-datalake",
    "pandas",
    "pymongo",
    "pyodbc",
    "python-dotenv",
    "ruff",
    "sqlalchemy",
```

Para instalar as bibliotecas acima através do `uv` basta seguir o comando abaixo:

```bash
uv venv # abrir o ambiente virtual caso nao tenha sido ativado ainda
source .venv/bin/activate # ativar o ambiente virtual para linux 
uv sync # instala as libs presente no pyproject.toml
```

Caso você não tenha a versão `3.12` para usar este projeto, basta executar o comando abaixo (usando o gerenciador de projetos python `uv`):

```bash
uv python install 3.12
```
ou usando `pyenv`:

```bash
pyenv install 3.12
pyenv local 3.12
```


Exemplo do arquivo `.env` que precisa ser criado para receber as credenciais de acesso ao SQL Server, Azure ADLS e ao MongoDB.

```
# Configurações do Azure Data Lake Storage
ADLS_ACCOUNT_NAME=datalake2aee089e227c8fc6
ADLS_FILE_SYSTEM_NAME=landing-zone
ADLS_DIRECTORY_NAME=dados
ADLS_SAS_TOKEN=chave_sas_token

# Configurações do SQL Server
SQL_SERVER=localhost
SQL_DATABASE=dados
SQL_SCHEMA=relacional
SQL_TABLE_NAME=sinistro
SQL_USERNAME=sa
SQL_PASSWORD=senha_sa_sqlserver

# Configurações do MongoDB Atlas
MONGODB_URI=mongodb+srv://...
MONGODB_DATABASE=sample_mflix
```

Estrutura de arquivos do projeto:

```
.
.
├── README.md
├── astro
│   ├── Dockerfile
│   ├── README.md
│   ├── airflow_settings.yaml
│   ├── dags
│   │   └── sqlserver_to_adls.py
│   ├── include
│   ├── packages.txt
│   ├── plugins
│   ├── requirements.txt
│   ├── sample.env
│   └── tests
├── docker
│   └── sqlserver
│       └── docker-compose.yaml
├── elt
│   ├── azure_integration
│   │   ├── __init__.py
│   │   └── adls_service.py
│   ├── database
│   │   ├── __init__.py
│   │   └── sql_server_service.py
│   └── main.py
├── examples
│   ├── elt_mongodb_n_collections.py
│   ├── elt_sql_1_tabela.py
│   └── elt_sql_n_tabelas.py
├── iac
│   └── adls
│       ├── README.md
│       ├── main.tf
│       ├── output.tf
│       ├── provider.tf
│       └── variables.tf
├── pyproject.toml
├── test
│   ├── test_connection_adls.py
│   ├── test_connection_mongodb.py
│   └── test_connection_sqlserver.py
└── uv.lock
```

Explicação da estrutura de pastas macro:
```
.
.
├── astro    <== Astro CLI Airflow
├── docker   <== Docker compose SQL Server
├── elt      <== Scripts Python ELT
├── examples <== Exemplos de código Python
├── iac      <== Terraform
└── test     <== Teste de conexao nos recursos
```


## Teste de Conectividade

Dentro da pasta `test` estão os arquivos para testar a conectividade no SQL Server e Azure ADLS.

```bash
# Ativar o ambiente virtual python usando o UV
uv venv
source .venv/bin/activate
```
```bash
uv run ./test/test_connection_adls.py
uv run ./test/test_connection_sqlserver.py
```

## Efetuando a cópia dos dados do SQL e jogando no Azure ADLS

1. Para copiar as tabelas do SQL Server e embarcar no Data Lake de maneira simples e pontual, na camada landing-zone, executar os comandos abaixo:

```bash
uv run ./elt_sql_n_tabelas.py
```

2. Para efetuar o mesmo processo, só que agora usando uma estrutura de Classes e métodos, escalável, use a estrutura abaixo:

```
└── elt
   ├── azure_integration
   │   ├── adls_service.py
   │   └── __init__.py
   ├── database
   │   ├── __init__.py
   │   └── sql_server_service.py
   └── main.py
```

```bash
uv run ./elt/main.py
```

Esta estrutura é escalável, pois caso necessite adicionar mais um banco de dados de origem, basta criar as classes e métodos do banco de dados em questão e adicionar o arquivo a pasta `database`.
A mesma dinâmica vale para o destino (sendo azure), na pasta `azure_integration`.
Caso seja alguma origem diferente de Azure, criar uma nova pasta dentro de `elt` e criar o arquivo de serviço para a tecnologia em questão.


## Troubleshooting I

> Se você estiver utilizando o S.O. Ubuntu para fazer a extração dos dados de um servidor SQL Server (através do PYODBC), é necessário instalar o driver ODBC do Microsoft SQL Server para Ubuntu (msodbcsql17). Esse driver permite que a conexão a uma instância do SQL Server a partir de ferramentas ou linguagens que usam ODBC.

### 1. Importar a chave GPG da Microsoft

```bash
curl https://packages.microsoft.com/keys/microsoft.asc | sudo apt-key add -
```
### 2. Adicionar o repositório da Microsoft

```bash
sudo add-apt-repository "$(curl https://packages.microsoft.com/config/ubuntu/$(lsb_release -rs)/prod.list)"
```
### 3. Atualizar a lista de pacotes

```bash
sudo apt-get update
```
### 4. Instalar o driver msodbcsql17

```bash
sudo apt-get install msodbcsql17
```
### 5. (Opcional) Instalar outras ferramentas relacionadas
Se você quiser instalar também ferramentas relacionadas, como o mssql-tools (que inclui o sqlcmd e o bcp), pode rodar o comando abaixo:

```bash
sudo apt-get install mssql-tools unixodbc-dev
```
### 6. Verificar a instalação

```bash
odbcinst -q -d -n "ODBC Driver 17 for SQL Server"
```
Se o driver foi instalado corretamente, você verá uma mensagem confirmando que o "ODBC Driver 17 for SQL Server" está disponível.

## Troubleshooting II

> Se voce estiver usando a versao 24.04 do Ubuntu e apareça esse warning...

    Warning: apt-key is deprecated. Manage keyring files in trusted.gpg.d instead (see apt-key(8)).

Seguir os passos abaixo:

### 1. Adicione a chave GPG ao local correto:
```
curl -fsSL https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor | sudo tee /etc/apt/trusted.gpg.d/microsoft.gpg > /dev/null
```
### 2. Adicione o repositório do Ubuntu 22.04 (Jammy), pois o 24.04 ainda não é suportado oficialmente:
```
echo "deb [arch=amd64] https://packages.microsoft.com/ubuntu/22.04/prod jammy main" | sudo tee /etc/apt/sources.list.d/microsoft-prod.list
```
### 3. Atualize os pacotes e instale o ODBC Driver 17:
```
sudo apt-get update
sudo ACCEPT_EULA=Y apt-get install -y msodbcsql17
```
Testar driver instalado:
```
odbcinst -q -d -n "ODBC Driver 17 for SQL Server"
```

## Configuração do AirFlow - Astro CLI

![image](https://github.com/user-attachments/assets/d5f69560-5997-4d00-a76a-dd239635d4d9)


### 1. Iniciar o Airflow (astro):

```
astro dev start
```

### 2. Abrir o Airflow UI (webserver):

```
localhost:8080
```

### 3. Reiniciar o AirFlow:
```
astro dev restart
```

### 4. Encerrar o AirFlow:
```
astro dev stop
```

## Observações sobre o ambiente do Airflow Astro CLI:

O `Dockerfile` foi personalizado para instalar o client *msodbcsql17* no container do airflow.  

Além disso, foram atualizados os arquivos `packages.txt` e `requirements.txt` com os pacotes/bibliotecas necessários para instalação nos containers do airflow.  

Na pasta `./dags` são criadas as dags (atividades) para que, a partir da airflow UI possa ser orquestrado (agendamentos, execuções em batch, etc).
Assim que elas são criadas na pasta /dags, elas aparecem automaticamente no airflow UI. Caso não apareçam ou estejam desatualizadas, você precisa reiniciar o airflow astro cli.

`pyproject.toml` contém as libs necesárias para executar os arquivos locais, como os arquivos das pastas `./tests`, `examples` e `elt`.

> Para executar as dags no airflow não é necessário ter as libs no `pyproject.toml`. Elas precisam estar no arquivo de `requirements.txt` (padrao do airflow astro cli para instalação dos mesmos nos containers do airflow).

