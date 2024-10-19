# Projeto para Extração de dados de um SQL Server e Carga em um Data Lake Storage da Azure (ADLS)


Para fazer a extração dos dados de um servidor SQL Server usando um Linux Ubuntu, através do PYODBC, você precisará instalar o driver ODBC do Microsoft SQL Server no Ubuntu (msodbcsql17), siga os passos abaixo. Esse driver permite que você se conecte a uma instância do SQL Server a partir de ferramentas ou linguagens que usam ODBC.

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
