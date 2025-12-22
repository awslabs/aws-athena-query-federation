import pymysql, json, logging, time
from abc import ABC, abstractmethod

from athena_federation_testing.util.client import cfn_client, rds_client, aws_client_factory, glue_job_client
from athena_federation_testing.util import config_helper
from athena_federation_testing.util.tpcds import tpcds_reader
from botocore.exceptions import ClientError


from sqlalchemy import create_engine, text
from athena_federation_testing.infra import common_infra
from athena_federation_testing.infra.federated_source import Federated_Source
import importlib.resources as resources
from athena_federation_testing.resources.infra import cfn_template

resources.files(cfn_template).joinpath("config.properties")


glue_job_glue_connection_name = "athena_integ_s3_to_mysql_glue_connection"



class JDBC_Source(Federated_Source, connection_type_name="jdbc", is_rds_source=False):

    @abstractmethod
    def create_jdbc_engine(self, with_db: bool=True):
        pass

    # for glue job per table start glue ETL
    def start_glue_job(self, table_name: list):
        pass

    def load_data_local(self) -> None:
        if self.is_rds():
            self._enable_rds_public_ccess()
            time.sleep(10)
        try:
            self.create_database_local(config_helper.get_tpcds_scale_factor())
            self.create_table_local(tpcds_reader.get_data_tables())
        except Exception as e:
            logging.error(f"🚨Create_data failed! for local mode: {e}")
        finally:
            if self.is_rds():
                self._disable_rds_public_ccess()

    def create_database_local(self, database_name: str) -> None:
        engine = self.create_jdbc_engine(False)
        with engine.connect() as con:
            con.execute(text(f"CREATE DATABASE IF NOT EXISTS {config_helper.get_tpcds_scale_factor()}"))
            logging.info(
                f"✅ Data source:{self.get_connection_type_name()}, Schema name:'{config_helper.get_tpcds_scale_factor()}' checked/created.")

    def create_table_local(self, table_names: list) -> None:
        engine = self.create_jdbc_engine()

        for table_name in table_names:
            df = tpcds_reader.load_tpcds_data_parquet_from_resources(table_name)
            logging.info(f"Data source:{self.get_connection_type_name()}, table:{table_name} Starting.")
            try:
                with engine.begin() as conn:
                    df.to_sql(name=f'{table_name}', con=conn, if_exists='append', index=False, method="multi",
                              chunksize=1000)
            except Exception as e:
                logging.error("Error during insert:", e)
            logging.info(f"✅ Data source:{self.get_connection_type_name()}, table:{table_name} checked/created.")
            with engine.connect() as con:
                con.execute(text(
                    f"ALTER TABLE `{table_name}` ADD PRIMARY KEY (`{tpcds_reader.get_tpcds_data_primary_key(table_name)}`, `{tpcds_reader.get_tpcds_data_sort_key(table_name)}`);"))
                logging.info(
                    f"✅ Updated primary key. Data source:{self.get_connection_type_name()}, table:{table_name} checked/created.")

    def create_database_s3(self, database_name: str) -> None:
        # instead of creating table individually, just batch create db with create talbe to avoid open public access for too long
        pass

    def create_table_s3(self, table_names: list) -> None:
        # below will require direct access, hence we do it separate.
        # We don't want to open public access (which could result in ticket) for too long hence just dropping table and create again.
        try:
            self._enable_rds_public_ccess()
            # print("put this back: self._enable_rds_public_ccess()")
            # DB might still busy, sleep for 10 sec
            time.sleep(10)
            # print("put this back: time.sleep(10)")
            self.create_database_local(config_helper.get_tpcds_scale_factor())

            engine = self.create_jdbc_engine(True)

            with engine.connect() as con:
                # if table exists, drop it as we will create a new one
                for table_name in table_names:
                    query = f"DROP TABLE IF EXISTS {table_name};"
                    con.execute(text(query))
                    logging.info(f"Query:{query} succceeded")
        except Exception as e:
            logging.error(f"🚨Create_data failed!: {e}")
            raise
        finally:
            self._disable_rds_public_ccess()
            # print("put this back: self._disable_rds_public_ccess()")

        # Create a glue connection for ETL
        self.create_glue_job()
        self.start_glue_job(table_names)
