from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from imdb_operators import ExportFilesOperator, ProcessFilesOperator, SaveToDatabaseOperator, CreateAnalyticalTablesOperator

# DAG
args_padrao = {
    'proprietario': 'airflow',
    'depende_do_passado': False,
    'data_inicio': datetime(2024, 3, 13),
    'enviar_email_em_caso_de_falha': False,
    'enviar_email_em_caso_de_repeticao': False,
    'repeticoes': 1,
    'intervalo_de_repeticao': timedelta(minutes=5),
}

dag = DAG(
    'imdb_pipeline',
    args=args_padrao,
    descricao='Pipeline do IMDB',
    intervalo_de_agendamento=timedelta(days=1),
)

# Define os diretórios e caminhos necessários
base_url = "https://datasets.imdbws.com/"
diretorio_dados = "/caminho/para/dados"
diretorio_tratados = "/caminho/para/tratados"
caminho_banco_de_dados = "/caminho/para/imdb_data.db"

# Define as tarefas da DAG
tarefa_exportar_arquivos = PythonOperator(
    task_id='exportar_arquivos',
    python_callable=ExportFilesOperator(
        task_id='exportar_arquivos',
        base_url=base_url,
        file_list=[
            "name.basics.tsv.gz",
            "title.akas.tsv.gz",
            "title.basics.tsv.gz",
            "title.crew.tsv.gz",
            "title.episode.tsv.gz",
            "title.principals.tsv.gz",
            "title.ratings.tsv.gz"
        ],
        destination_directory=diretorio_dados,
        dag=dag,
    ).execute,
    dag=dag,
)

tarefa_processar_arquivos = PythonOperator(
    task_id='processar_arquivos',
    python_callable=ProcessFilesOperator(
        task_id='processar_arquivos',
        source_directory=diretorio_dados,
        destination_directory=diretorio_tratados,
        file_extension=".gz",
        dag=dag,
    ).execute,
    dag=dag,
)

tarefa_salvar_no_banco_de_dados = PythonOperator(
    task_id='salvar_no_banco_de_dados',
    python_callable=SaveToDatabaseOperator(
        task_id='salvar_no_banco_de_dados',
        source_directory=diretorio_tratados,
        database_path=caminho_banco_de_dados,
        dag=dag,
    ).execute,
    dag=dag,
)

tarefa_criar_tabelas_analiticas = PythonOperator(
    task_id='criar_tabelas_analiticas',
    python_callable=CreateAnalyticalTablesOperator(
        task_id='criar_tabelas_analiticas',
        queries=[
            """
            CREATE TABLE IF NOT EXISTS analitico_titulos AS
            -- Sua query aqui
            """,
            """
            CREATE TABLE IF NOT EXISTS analitico_participantes AS
            -- Sua query aqui
            """
        ],
        database_path=caminho_banco_de_dados,
        dag=dag,
    ).execute,
    dag=dag,
)

# Define a ordem de execução das tarefas
tarefa_exportar_arquivos >> tarefa_processar_arquivos >> tarefa_salvar_no_banco_de_dados >> tarefa_criar_tabelas_analiticas
