"""
Orchestrateur Apache Beam + schedule
- Exécute un job Beam toutes les 10 minutes
- Le job peut lancer: export Data Lake, sync Data Warehouse, ou produire dans Kafka
- DirectRunner (local). Compatible Dataflow avec options appropriées.
"""
import argparse
import logging
import os
import sys
import time
from datetime import datetime
from typing import Callable

import schedule

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions

# Tâches importées
# Nous appelons directement les scripts existants via des fonctions wrapper pour simplicité


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - beam_orchestrator - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("beam_orchestrator")


def task_export_datalake() -> str:
    import subprocess
    cmd = [sys.executable, "export_to_data_lake.py", "--all"]
    res = subprocess.run(cmd, capture_output=True, text=True)
    return res.stdout[-5000:]


def task_sync_warehouse(mysql_password: str) -> str:
    import subprocess
    cmd = [sys.executable, "sync_to_mysql.py", "--mysql-password", mysql_password]
    res = subprocess.run(cmd, capture_output=True, text=True)
    return res.stdout[-5000:]


def task_kafka_produce(topic: str, messages: int, rate: int) -> str:
    import subprocess
    cmd = [
        sys.executable,
        "kafka_producer.py",
        "--topic",
        topic,
        "--messages",
        str(messages),
        "--rate",
        str(rate),
    ]
    res = subprocess.run(cmd, capture_output=True, text=True)
    return res.stdout[-5000:]


class RunTask(beam.DoFn):
    def __init__(self, fn: Callable[[], str], name: str):
        self.fn = fn
        self.name = name

    def process(self, element):
        start = datetime.utcnow()
        logger.info(f"[Beam] Démarrage tâche: {self.name}")
        out = self.fn()
        end = datetime.utcnow()
        yield {
            "task": self.name,
            "start": start.isoformat()+"Z",
            "end": end.isoformat()+"Z",
            "output_tail": out,
        }


def run_beam_job(task_name: str, mysql_password: str, topic: str, messages: int, rate: int,
                 runner: str = "DirectRunner", project: str = None, region: str = None,
                 temp_location: str = None, staging_location: str = None,
                 service_account: str = None):
    # Construire dynamiquement les options Beam pour supporter DirectRunner et DataflowRunner
    flags = [
        f"--runner={runner}",
        "--save_main_session=True",
    ]
    if runner.lower() == "dataflowrunner":
        if project: flags += [f"--project={project}"]
        if region: flags += [f"--region={region}"]
        if temp_location: flags += [f"--temp_location={temp_location}"]
        if staging_location: flags += [f"--staging_location={staging_location}"]
        if service_account: flags += [f"--service_account_email={service_account}"]

    options = PipelineOptions(flags=flags)
    options.view_as(SetupOptions).save_main_session = True

    if task_name == "export_datalake":
        fn = lambda: task_export_datalake()
    elif task_name == "sync_warehouse":
        fn = lambda: task_sync_warehouse(mysql_password)
    elif task_name == "kafka_produce":
        fn = lambda: task_kafka_produce(topic, messages, rate)
    else:
        raise ValueError(f"Tâche inconnue: {task_name}")

    with beam.Pipeline(options=options) as p:
        (
            p
            | "CreateSingle" >> beam.Create([1])
            | f"Run {task_name}" >> beam.ParDo(RunTask(fn, task_name))
            | "Log" >> beam.Map(lambda x: logger.info(f"[Beam] Résultat: {x}"))
        )


def schedule_loop(task_name: str, every_minutes: int, mysql_password: str, topic: str, messages: int, rate: int,
                  runner: str, project: str, region: str, temp_location: str, staging_location: str, service_account: str):
    def job():
        logger.info(f"Planification: lancement job '{task_name}'")
        run_beam_job(task_name, mysql_password, topic, messages, rate,
                     runner=runner, project=project, region=region,
                     temp_location=temp_location, staging_location=staging_location,
                     service_account=service_account)

    schedule.every(every_minutes).minutes.do(job)
    logger.info(f"Scheduler démarré - fréquence: {every_minutes} minutes")
    job()  # Lancer immédiatement la première exécution
    while True:
        schedule.run_pending()
        time.sleep(1)


def main():
    parser = argparse.ArgumentParser(description="Orchestrateur Apache Beam + schedule (toutes les 10 min)")
    parser.add_argument("--task", choices=["export_datalake", "sync_warehouse", "kafka_produce"], required=True)
    parser.add_argument("--every-minutes", type=int, default=10)
    parser.add_argument("--mysql-password", type=str, default="")
    parser.add_argument("--topic", type=str, default="transaction_stream")
    parser.add_argument("--messages", type=int, default=5000)
    parser.add_argument("--rate", type=int, default=1000)
    # Options runner
    parser.add_argument("--runner", type=str, default="DirectRunner", help="DirectRunner ou DataflowRunner")
    parser.add_argument("--project", type=str, default=None)
    parser.add_argument("--region", type=str, default=None)
    parser.add_argument("--temp_location", type=str, default=None)
    parser.add_argument("--staging_location", type=str, default=None)
    parser.add_argument("--service_account", type=str, default=None)

    args = parser.parse_args()
    schedule_loop(
        task_name=args.task,
        every_minutes=args.every_minutes,
        mysql_password=args.mysql_password,
        topic=args.topic,
        messages=args.messages,
        rate=args.rate,
        runner=args.runner,
        project=args.project,
        region=args.region,
        temp_location=args.temp_location,
        staging_location=args.staging_location,
        service_account=args.service_account,
    )


if __name__ == "__main__":
    main()
