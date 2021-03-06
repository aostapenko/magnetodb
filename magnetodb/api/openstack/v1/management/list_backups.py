# Copyright 2014 Mirantis Inc.
# All Rights Reserved.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

from magnetodb.api import validation
from magnetodb.api.openstack.v1 import parser
from magnetodb.api.openstack.v1 import utils
from magnetodb.common import probe


class ListBackupsController(object):
    """ Deletes a backup. """

    @probe.Probe(__name__)
    def process_request(self, req, project_id, table_name):
        utils.check_project_id(req.context, project_id)
        req.context.tenant = project_id

        validation.validate_table_name(table_name)

        params = req.params.copy()

        # exclusive_start_backup_id = params.pop(
        #     parser.Props.EXCLUSIVE_START_BACKUP_ID, None)

        limit = params.pop(parser.Props.LIMIT, None)
        if limit:
            limit = validation.validate_integer(limit, parser.Props.LIMIT,
                                                min_val=0)

        backups = []
        response = {}

        if backups and str(limit) == str(len(backups)):
            response[parser.Props.LAST_EVALUATED_BACKUP_ID] = backups[-1].id

        self_link_prefix = req.path_url

        response[parser.Props.BACKUPS] = [
            parser.Parser.format_backup(backup, self_link_prefix)
            for backup in backups
        ]

        return response
