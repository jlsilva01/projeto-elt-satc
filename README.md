# Projeto para Extração de dados de um SQL Server e Carga em um Data Lake Storage da Azure (ADLS)

Este projeto utiliza a versão `3.12` do Python, o gerenciador de projeto em Python `uv` (Universal Versioning) e as seguintes bibliotecas:

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
A versão de cada biblioteca dependerá da sua versão do Python.

Para instalar as bibliotecas acima através do `uv` basta seguir o comando abaixo:

```bash
uv venv # abrir o ambiente virtual caso nao tenha sido ativado ainda
source .venv/bin/activate # ativar o ambiente virtual
uv add azure-core azure-identity azure-storage-file-datalake pandas pymongo pyodbc python-dotenv ruff sqlalchemy
```

Caso você não tenha a versão `3.12` para usar este projeto, basta executar o comando abaixo (usando o gerenciador de projetos python `uv`):

```bash
uv python install 3.12
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

MONGODB_URI=mongodb+srv://usuario:senha@m0-cluster-dev-data-eng.hkyhs91.mongodb.net/
MONGODB_DATABASE=sample_mflix
```

Estrutura de arquivos do projeto:

```
.
├── elt
│   ├── azure_integration
│   │   ├── adls_service.py
│   │   ├── __init__.py
│   │   └── __pycache__
│   │       ├── adls_service.cpython-312.pyc
│   │       └── __init__.cpython-312.pyc
│   ├── database
│   │   ├── __init__.py
│   │   ├── __pycache__
│   │   │   ├── __init__.cpython-312.pyc
│   │   │   └── sql_server_service.cpython-312.pyc
│   │   └── sql_server_service.py
│   └── main.py
├── elt_mongodb_n_collections.py
├── elt_sql_1_tabela.py
├── elt_sql_n_tabelas.py
├── examples
│   ├── elt_mongodb_n_collections.py
│   ├── elt_sql_1_tabela.py
│   └── elt_sql_n_tabelas.py
├── pyproject.toml
├── README.md
├── test
│   ├── test_connection_adls.py
│   └── test_connection_sqlserver.py
└── uv.lock
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
A mesma dinâmica vale para o destino (sendo azure), na pasta ´azure_integration`.
Caso seja alguma origem diferente de Azure, criar uma nova pasta dentro de `elt` e criar o arquivo de serviço para a tecnologia em questão.


## Troubleshooting

Caso você esteja utilizando o S.O. Ubuntu, para fazer a extração dos dados de um servidor SQL Server (através do PYODBC), é necessário instalar o driver ODBC do Microsoft SQL Server para Ubuntu (msodbcsql17). Esse driver permite que a conexão a uma instância do SQL Server a partir de ferramentas ou linguagens que usam ODBC.

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
