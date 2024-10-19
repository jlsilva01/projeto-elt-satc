# Projeto para Extração de dados de um SQL Server e Carga em um Data Lake Storage da Azure (ADLS)

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

Dentro da pasta `test` estão os arquivos para testar a conectividade no SQL Server e Azure ADLS.

Para executar o arquivos, siga os passos abaixo:

```bash
# Ativar o ambiente virtual python usando o UV
uv venv
source .venv/bin/activate
```
```bash
uv run ./test/test_connection_adls.py
uv run ./test/test_connection_sqlserver.py
```

Para copiar as tabelas do SQL Server e embarcar no Data Lake, na camada landing-zone, executar os comandos abaixo:

```bash
uv run ./elt_sql_n_tabelas.py
```


## Troubleshooting

Para fazer a extração dos dados de um servidor SQL Server usando um Linux Ubuntu, através do PYODBC, você precisará instalar o driver ODBC do Microsoft SQL Server no Ubuntu (msodbcsql17). Esse driver permite que você se conecte a uma instância do SQL Server a partir de ferramentas ou linguagens que usam ODBC.

### 1. Importar a chave GPG da Microsoft
Isso é necessário para que o sistema confie nos pacotes da Microsoft.

```bash
curl https://packages.microsoft.com/keys/microsoft.asc | sudo apt-key add -
```
### 2. Adicionar o repositório da Microsoft
Adicione o repositório do SQL Server para a versão do Ubuntu que você está utilizando. Este comando obtém o arquivo de configuração do repositório e o adiciona ao sistema.

```bash
sudo add-apt-repository "$(curl https://packages.microsoft.com/config/ubuntu/$(lsb_release -rs)/prod.list)"
```
### 3. Atualizar a lista de pacotes
Após adicionar o repositório, atualize os pacotes do sistema:

```bash
sudo apt-get update
```
### 4. Instalar o driver msodbcsql17
Agora você pode instalar o driver ODBC para o SQL Server (msodbcsql17):

```bash
sudo apt-get install msodbcsql17
```
### 5. (Opcional) Instalar outras ferramentas relacionadas
Se você quiser instalar também ferramentas relacionadas, como o mssql-tools (que inclui o sqlcmd e o bcp), pode rodar o comando abaixo:

```bash
sudo apt-get install mssql-tools unixodbc-dev
```
### 6. Verificar a instalação
Você pode verificar se o driver foi instalado corretamente listando os drivers ODBC disponíveis:

```bash
odbcinst -q -d -n "ODBC Driver 17 for SQL Server"
```
Se o driver foi instalado corretamente, você verá uma mensagem confirmando que o "ODBC Driver 17 for SQL Server" está disponível.
