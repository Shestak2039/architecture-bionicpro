import csv
import os
import shutil
from datetime import date, timedelta

import clickhouse_connect
import pendulum
from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

TMP_DIR = '/tmp/reports_etl'

MART_COLUMNS = [
    'user_id', 'report_date', 'prosthesis_id', 'prosthesis_model', 'client_name', 'country',
    'movements_count', 'avg_reaction_ms', 'max_reaction_ms', 'recognition_errors',
    'avg_battery_level',
]


def clickhouse_client():
    conn = BaseHook.get_connection('clickhouse_reports')
    return clickhouse_connect.get_client(
        host=conn.host, port=conn.port, username=conn.login,
        password=conn.password, database=conn.schema,
    )


def run_dir(run_id: str) -> str:
    path = os.path.join(TMP_DIR, run_id.replace(':', '_').replace('+', '_'))
    os.makedirs(path, exist_ok=True)
    return path


def get_period(data_interval_end, **_):
    period_end = data_interval_end.date() - timedelta(days=1)

    loaded_until = clickhouse_client().command('SELECT max(loaded_until) FROM etl_loaded_periods')
    if str(loaded_until) != '1970-01-01':
        period_start = date.fromisoformat(str(loaded_until)) + timedelta(days=1)
    else:
        first_day = PostgresHook('telemetry_db').get_first(
            'SELECT min(event_time)::date FROM prosthesis_telemetry')[0]
        if first_day is None:
            raise AirflowSkipException('В источнике нет телеметрии')
        period_start = first_day

    if period_start > period_end:
        raise AirflowSkipException(f'Витрина уже загружена до {period_end}')

    print(f'Период загрузки: {period_start} - {period_end}')
    return {'start': period_start.isoformat(), 'end': period_end.isoformat()}


def extract_telemetry(ti, run_id, **_):
    period = ti.xcom_pull(task_ids='get_period')
    sql = f"""
        COPY (
            SELECT prosthesis_id, event_time::date AS event_date, event_type,
                   reaction_ms, battery_level
            FROM prosthesis_telemetry
            WHERE event_time >= DATE '{period['start']}'
              AND event_time <  DATE '{period['end']}' + 1
        ) TO STDOUT WITH CSV HEADER
    """
    PostgresHook('telemetry_db').copy_expert(sql, os.path.join(run_dir(run_id), 'telemetry.csv'))


def extract_crm(run_id, **_):
    sql = """
        COPY (
            SELECT p.prosthesis_id, p.model, c.user_login, c.full_name, c.country
            FROM prostheses p
            JOIN clients c ON c.client_id = p.client_id
        ) TO STDOUT WITH CSV HEADER
    """
    PostgresHook('crm_db').copy_expert(sql, os.path.join(run_dir(run_id), 'crm.csv'))


def transform(run_id, **_):
    path = run_dir(run_id)

    with open(os.path.join(path, 'crm.csv'), newline='', encoding='utf-8') as f:
        crm = {row['prosthesis_id']: row for row in csv.DictReader(f)}

    stats = {}
    with open(os.path.join(path, 'telemetry.csv'), newline='', encoding='utf-8') as f:
        for row in csv.DictReader(f):
            key = (row['prosthesis_id'], row['event_date'])
            s = stats.setdefault(key, {'movements': 0, 'reaction_sum': 0, 'reaction_max': 0,
                                       'errors': 0, 'battery_sum': 0, 'events': 0})
            s['events'] += 1
            s['battery_sum'] += int(row['battery_level'])
            if row['event_type'] == 'movement':
                reaction = int(row['reaction_ms'])
                s['movements'] += 1
                s['reaction_sum'] += reaction
                s['reaction_max'] = max(s['reaction_max'], reaction)
            elif row['event_type'] == 'recognition_error':
                s['errors'] += 1

    unknown = set()
    with open(os.path.join(path, 'mart.csv'), 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(MART_COLUMNS)
        for (prosthesis_id, event_date), s in sorted(stats.items()):
            client = crm.get(prosthesis_id)
            if client is None:
                unknown.add(prosthesis_id)
                continue
            writer.writerow([
                client['user_login'], event_date, prosthesis_id, client['model'],
                client['full_name'], client['country'],
                s['movements'],
                round(s['reaction_sum'] / s['movements'], 1) if s['movements'] else 0,
                s['reaction_max'],
                s['errors'],
                round(s['battery_sum'] / s['events'], 1),
            ])

    if unknown:
        print(f'Протезы без клиента в CRM пропущены: {sorted(unknown)}')


def load(ti, run_id, **_):
    period = ti.xcom_pull(task_ids='get_period')

    rows = []
    with open(os.path.join(run_dir(run_id), 'mart.csv'), newline='', encoding='utf-8') as f:
        for r in csv.DictReader(f):
            rows.append([
                r['user_id'], date.fromisoformat(r['report_date']), r['prosthesis_id'],
                r['prosthesis_model'], r['client_name'], r['country'],
                int(r['movements_count']), float(r['avg_reaction_ms']), int(r['max_reaction_ms']),
                int(r['recognition_errors']), float(r['avg_battery_level']),
            ])

    client = clickhouse_client()
    client.command(
        'ALTER TABLE reports_mart DELETE WHERE report_date BETWEEN %(start)s AND %(end)s',
        parameters=period, settings={'mutations_sync': 2},
    )
    if rows:
        client.insert('reports_mart', rows, column_names=MART_COLUMNS)
    print(f'Загружено строк в витрину: {len(rows)}')


def mark_loaded(ti, run_id, **_):
    period = ti.xcom_pull(task_ids='get_period')
    clickhouse_client().insert(
        'etl_loaded_periods', [[date.fromisoformat(period['end'])]], column_names=['loaded_until'])
    shutil.rmtree(run_dir(run_id), ignore_errors=True)


with DAG(
    dag_id='reports_mart',
    description='Телеметрия + CRM -> витрина отчётов в ClickHouse',
    schedule='0 2 * * *',
    start_date=pendulum.datetime(2026, 10, 1, tz='UTC'),
    catchup=False,
    max_active_runs=1,
    default_args={'retries': 2, 'retry_delay': timedelta(minutes=5)},
    tags=['reports', 'etl'],
) as dag:
    get_period_task = PythonOperator(task_id='get_period', python_callable=get_period)
    extract_telemetry_task = PythonOperator(task_id='extract_telemetry', python_callable=extract_telemetry)
    extract_crm_task = PythonOperator(task_id='extract_crm', python_callable=extract_crm)
    transform_task = PythonOperator(task_id='transform', python_callable=transform)
    load_task = PythonOperator(task_id='load', python_callable=load)
    mark_loaded_task = PythonOperator(task_id='mark_loaded', python_callable=mark_loaded)

    get_period_task >> [extract_telemetry_task, extract_crm_task] >> transform_task >> load_task >> mark_loaded_task
