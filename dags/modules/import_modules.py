
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.hooks.base import BaseHook
from airflow.providers.ssh.operators.ssh import SSHOperator
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
from airflow.models import DagRun, Variable
from airflow import settings, AirflowException
from sqlalchemy import func
import contextlib
import glob
import os
import chardet


def parse_sql_file(sql_file_path):
	if not sql_file_path:
		raise ValueError("sql_file_path is required")
	if not os.path.exists(sql_file_path):
		raise FileNotFoundError(f"SQL file not found: {sql_file_path}")
	with open(sql_file_path, "r", encoding="utf-8") as sql_file:
		content = sql_file.read().strip()
	content = content.rstrip(";")
	if not content:
		raise ValueError(f"SQL file is empty: {sql_file_path}")
	return content