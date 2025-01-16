from prefect import flow, task
import os
import sys
import importlib

sys.path.insert(0, os.path.abspath("./icdc-dataloader"))
icdc_dataloader = importlib.import_module("icdc-dataloader")

from icdc_dataloader.loader import main
from config import PluginConfig
from icdc_dataloader.bento.common.secret_manager import get_secret
from typing import Literal

NEO4J_URI = "neo4j_uri"
NEO4J_PASSWORD = "neo4j_password"
SUBMISSION_BUCKET = "submission_bucket"

DropDownChoices = Literal[True, False]
ModeDropDownChoices = Literal["upsert", "new", "delete"]

@flow(name="Data Loader", log_prints=True)
def load_data(
        s3_bucket,
        s3_folder,
        upload_log_dir = None,
        dataset = "data",
        temp_folder = "tmp",
        uri = "bolt://127.0.0.1:7687",
        user = "neo4j",
        password = "123456",
        schemas = ["../ccdi-model/model-desc/icdc-model.yml", "../ccdi-model/model-desc/icdc-model-props.yml"],
        prop_file = "./icdc-dataloader/config/props-ccdi-model.yml",
        backup_folder = None,
        cheat_mode = False,
        dry_run = False,
        wipe_db = False,
        no_backup = True,
        no_parents = True,
        verbose = False,
        yes = True,
        max_violation = 1000000,
        mode = "upsert",
        split_transaction = False,
        plugins = []
    ):

    params = Config(
        dataset,
        uri,
        user,
        password,
        schemas,
        prop_file,
        s3_bucket,
        s3_folder,
        backup_folder,
        cheat_mode,
        dry_run,
        wipe_db,
        no_backup,
        no_parents,
        verbose,
        yes,
        max_violation,
        mode,
        split_transaction,
        upload_log_dir,
        plugins,
        temp_folder
    )
    main(params)

class Config:
    def __init__(
            self,
            dataset,
            uri,
            user,
            password,
            schemas,
            prop_file,
            bucket,
            s3_folder,
            backup_folder,
            cheat_mode,
            dry_run,
            wipe_db,
            no_backup,
            no_parents,
            verbose,
            yes,
            max_violation,
            mode,
            split_transaction,
            upload_log_dir,
            plugins,
            temp_folder
    ):
        self.dataset = dataset
        self.uri = uri
        self.user = user
        self.password = password
        self.schema = schemas
        self.prop_file = prop_file
        self.bucket = bucket
        self.s3_folder = s3_folder
        self.backup_folder = backup_folder
        self.cheat_mode = cheat_mode
        self.dry_run = dry_run
        self.wipe_db = wipe_db
        self.no_backup = no_backup
        self.no_parents = no_parents
        self.verbose = verbose
        self.yes = yes
        self.max_violations = max_violation
        self.mode = mode
        self.split_transactions = split_transaction
        self.upload_log_dir = upload_log_dir
        self.plugins = []
        self.temp_folder = temp_folder
        for plugin in plugins:
            self.plugins.append(PluginConfig(plugin))

        self.config_file = None


@flow(name="CCDI Hub Data Loader", log_prints=True)
def ccdi_hub_data_loader(
        secret_name: str,
        metadata_folder: str,
        schemas: list[str],
        prop_file: str,
        cheat_mode: DropDownChoices,
        dry_run: DropDownChoices,
        wipe_db: DropDownChoices,
        mode: ModeDropDownChoices,
        split_transaction: DropDownChoices,
        plugins=[],
        no_backup: bool = True,
        yes: bool = True,
        max_violation: int = 1000000,
    ):
    """Entrypoint of prefect data loader for CCDI sandbox DB

    Args:
        secret_name (str): secret name stored in AWS secrets manager.
        metadata_folder (str): folder path of metadata under hard coded s3 bucket.
        schemas (list[str]): List of ccdi data model files.
        prop_file (str): path of props-ccdi-model.yml.
        cheat_mode (DropDownChoices): If turn on cheat mode.
        dry_run (DropDownChoices): if dry run.
        wipe_db (DropDownChoices): if wipe the entire database.
        mode (ModeDropDownChoices): data loading mode.
        split_transaction (DropDownChoices): if split transaction.
        plugins (list, optional): Defaults to [].
        no_backup (bool, optional): Defaults to True.
        yes (bool, optional): Defaults to True.
        max_violation (int, optional): Defaults to 1000000.
    """    

    secret = get_secret(secret_name)
    uri = secret[NEO4J_URI]
    password = secret[NEO4J_PASSWORD]
    s3_bucket = secret[SUBMISSION_BUCKET]
    s3_folder = f'/{metadata_folder}'

    load_data(
        s3_bucket = s3_bucket,
        s3_folder = s3_folder,
        upload_log_dir = f's3://{s3_bucket}/{s3_folder}/logs',
        dataset = "data",
        temp_folder = "tmp",
        uri = uri,
        password = password,
        schemas = schemas,
        prop_file = prop_file,
        cheat_mode = cheat_mode,
        dry_run = dry_run,
        wipe_db = wipe_db,
        no_backup = no_backup,
        yes = yes,
        max_violation = max_violation,
        mode = mode,
        split_transaction = split_transaction,
        plugins = plugins
    )

if __name__ == "__main__":
    # create your first deployment
    load_data.serve(name="local-data-loader-deployment")
