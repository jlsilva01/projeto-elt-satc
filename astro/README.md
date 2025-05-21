### Iniciar o airflow (astro):

```
astro dev start
```

### Abrir o airflow:

```
localhost:8080
```

Credenciais padrão:

usuario: airflow | senha: airflow

Pastas:

.
├── Dockerfile
├── README.md
├── airflow_settings.yaml
├── dags
│   ├── sqlserver_to_adls.py
│   ├── validate_adls_connection.py
│   └── validate_mssql_conenction.py
├── examples
│   └── elt_sql_n_tabelas.py
├── include
├── packages.txt
├── plugins
├── pyproject.toml
├── requirements.txt
├── sample.env
├── tests
│   └── dags
│       └── test_dag_example.py
└── uv.lock

O `Dockerfile` foi personalizado para instalar o client msodbcsql17 no container do astro.

Além disso, foi atualizado os arquivos packages.txt e requirements.txt com os pacotes/bibliotecas necessários para instalacao nos containers.

Na pasta `/dags` sao criadas as dags para que, a partir da UI do airflow possa ser agendado e efetuada orquestracao das atividades.

pyproject.toml contém as libs necesários para executar os arquivos locais, como o ./examples/elt_sql_n_tabelas.py.

Já para executar as dags no airflow não é necessário ter as libs no pyproject.toml. Apenas precisam estar no arquivo de requirements.txt (padrao do astro cli para instalacao dos mesmos nos containers do airflow).

