# Copyright 2014 Symantec Corporation
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

import copy
import time

from tempest.api.keyvalue.rest_base.base import MagnetoDBTestCase
from tempest.common.utils.data_utils import rand_name
from tempest import exceptions
from tempest.test import attr


class MagnetoDBListTableTestCase(MagnetoDBTestCase):

    def setUp(self):
        super(MagnetoDBListTableTestCase, self).setUp()
        self.tables = []

    def tearDown(self):
        for tname in self.tables:
            try:
                self.client.delete_table(tname)
            except (exceptions.BadRequest, exceptions.NotFound):
                self.tables.remove(tname)
        while True:
            for tname in self.tables:
                try:
                    self.client.describe_table(tname)
                except exceptions.NotFound:
                    self.tables.remove(tname)
            if not self.tables:
                break
            time.sleep(1)
        super(MagnetoDBListTableTestCase, self).tearDown()

    @attr(type=['LisT-1'])
    def test_list_tables_empty(self):
        headers, body = self.client.list_tables()
        expected = {'tables': []}
        self.assertEqual(body, expected)

    @attr(type=['LisT-2'])
    def test_list_tables(self):
        tname = rand_name().replace('-', '')
        self._create_test_table(self.smoke_attrs, tname,
                                self.smoke_schema,
                                wait_for_active=True,
                                cleanup=False)
        self.tables.append(tname)
        headers, body = self.client.list_tables()
        expected = {'tables': [{'href': tname, 'rel': 'self'}]}
        self.assertEqual(body, expected)

    def _create_n_tables(self, num):
        for i in range(0, num):
            tname = 'aaa' + str(i)#rand_name().replace('-', '')
            self._create_test_table(self.smoke_attrs, tname,
                                    self.smoke_schema,
                                    cleanup=False)
            self.tables.append(tname)
        creating_tables = copy.copy(self.tables)
        t = time.time()
        while time.time() - t < 120:
            for tname in creating_tables:
                headers, body = self.client.describe_table(tname)
                if body['table']['table_status'] == 'ACTIVE':
                    creating_tables.remove(tname)
            if not creating_tables:
                break
            time.sleep(1)

    @attr(type=['LisT-10'])
    def test_list_tables_no_limit_5_tables(self):
        tnum = 40
        self._create_n_tables(tnum)
        headers, body = self.client.list_tables()
        self.assertEqual(len(body['tables']), tnum)
