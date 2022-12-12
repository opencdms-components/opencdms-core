###############################################################################
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
###############################################################################
__version__ = "0.0.1"

import logging
import subprocess
from pygeoapi.process.base import BaseProcessor, ProcessorExecuteError

LOGGER = logging.getLogger(__name__)

PROCESS_METADATA = {
    "version": "0.0.1",
    "id": "opencdms_schedule",
    "title": {
        "en": "OpenCDMS Schedule",
    },
    "description": {
        "en": "This pygeoapi process helps set up scheduled jobs.",
    },
    "keywords": [],
    "links": [
        {
            "type": "text/html",
            "rel": "about",
            "title": "information",
            "href": "https://example.org/process",
            "hreflang": "en-US",
        }
    ],
    "inputs": {
        "command": {
            "title": "Command",
            "description": "Command to run using crontab",
            "schema": {"type": "string"},
            "minOccurs": 1,
            "maxOccurs": 1,
            "metadata": None,
            "keywords": [],
        },
        "cron_expression": {
            "title": "CRON expression",
            "description": (
                "Optional cron expression to set up periodic job. If not"
                " provided, the backup job will run everyday at midnight"
                " (UTC)."
            ),
            "schema": {"type": "string"},
            "minOccurs": 0,
            "maxOccurs": 1,
            "metadata": None,
            "keywords": [],
        },
    },
    "outputs": {
        "message": {
            "title": "Job execution status",
            "schema": {"type": "string"},
        },
        "deployment_key": {
            "title": "Deployment key",
            "schema": {"type": "string"},
        },
        "crontab_entry": {
            "title": "Crontab entry",
            "schema": {"type": "string"},
        },
    },
    "example": {
        "inputs": {
            "command": (
                'curl -X POST'
                ' http://localhost:5000/processes/opencdms_backup/execution'
                r' -H \"accept: application/json\" -H \"Content-Type:'
                r' application/json\" -d \'{\"inputs\": {\"deployment_key\":'
                r' \"test-database\",\"output_dir\":'
                r' \"/home/faysal/PycharmProjects/opencdms-backup\"},\"mode\":'
                r' \"async\"}\''
            ),
            "cron_expression": "* * * * *",
        }
    },
}


def existing_cron_jobs():
    process = subprocess.Popen(
        ["sh", "-c", "crontab -l"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    stdout, stderr = process.communicate()
    return set(
        filter(
            lambda x: bool(x),
            [c.strip() for c in stdout.decode("utf-8").split("\n")],
        )
    )


class OpenCDMSSchedule(BaseProcessor):
    def __init__(self, processor_def):
        """
        Initialize object
        :param processor_def: provider definition
        :returns: pygeoapi.process.opencdms_backup.OpenCDMSBackup
        """

        super().__init__(processor_def, PROCESS_METADATA)

    def execute(self, data):
        mimetype = "application/json"
        try:
            cron_expression = data.get("cron_expression", "00 00 * * *")
            command = data["command"]

            crontab_entry = f"{cron_expression} {command}"

            if crontab_entry in existing_cron_jobs():
                raise ProcessorExecuteError(
                    "Cron job already exists for same schedule."
                )

            commands = [
                ["sh", "-c", "crontab -l > tmp_cron"],
                [
                    "sh",
                    "-c",
                    f'echo "{crontab_entry}" >> tmp_cron',
                ],
                ["crontab", "tmp_cron"],
                ["rm", "tmp_cron"],
            ]

            output = {
                "message": "Process scheduled successfully.",
                "crontab_entry": crontab_entry,
            }

            for command in commands:
                process = subprocess.Popen(
                    command, stdout=subprocess.PIPE, stderr=subprocess.PIPE
                )
                stdout, stderr = process.communicate()
                logging.info(stdout.decode("utf-8"))
                logging.info(stderr.decode("utf-8"))
                if stderr:
                    output = {"message": "Failed scheduling process."}
                    break

        except KeyError as e:
            LOGGER.exception(e)
            output = {"message": f"Required field: {str(e)}"}
        except (AttributeError, ValueError) as e:
            LOGGER.exception(e)
            output = {"message": "Invalid db connection string."}
        except ProcessorExecuteError as e:
            output = {"message": str(e)}
        except Exception as e:
            LOGGER.exception(e)
            output = {"message": "Failed scheduling process."}
        return mimetype, output

    def __repr__(self):
        return "<OpenCDMSSchedule> {}".format(self.name)
