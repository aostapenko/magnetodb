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

import time

from tempest.api.keyvalue.rest_base.base import MagnetoDBTestCase
from tempest.common.utils.data_utils import rand_name
from tempest import exceptions
from tempest import test


ATTRIBUTE_DEFINITIONS = [
    {
        "attribute_name": "hash_attr",
        "attribute_type": "S"
    },
    {
        "attribute_name": "range_attr",
        "attribute_type": "S"
    },
    {
        "attribute_name": "istr",
        "attribute_type": "S"
    },
    {
        "attribute_name": "inumber",
        "attribute_type": "N"
    },
    {
        "attribute_name": "iblob",
        "attribute_type": "B"
    }
]

KEY_SCHEMA = [
    {
        "attribute_name": "hash_attr",
        "key_type": "HASH"
    },
    {
        "attribute_name": "range_attr",
        "key_type": "RANGE"
    }
]

LSI_INDEXES = [
    {
        "index_name": "by_str",
        "key_schema": [
            {
                "attribute_name": "hash_attr",
                "key_type": "HASH"
            },
            {
                "attribute_name": "istr",
                "key_type": "RANGE"
            }
        ],
        "projection": {
            "projection_type": "ALL"
        }
    },
    {
        "index_name": "by_number",
        "key_schema": [
            {
                "attribute_name": "hash_attr",
                "key_type": "HASH"
            },
            {
                "attribute_name": "inumber",
                "key_type": "RANGE"
            }
        ],
        "projection": {
            "projection_type": "ALL"
        }
    },
    {
        "index_name": "by_blob",
        "key_schema": [
            {
                "attribute_name": "hash_attr",
                "key_type": "HASH"
            },
            {
                "attribute_name": "iblob",
                "key_type": "RANGE"
            }
        ],
        "projection": {
            "projection_type": "ALL"
        }
    }
]

TABLE_NAME = 'atable'

ITEM_PRIMARY_KEY = {
    "hash_attr": {"S": "1"},
    "range_attr": {"S": "1"}
}

KEY_CONDITIONS = {
    "hash_attr": {
        "attribute_value_list": [{"S": "1"}],
        "comparison_operator": "EQ"
    },
    "range_attr": {
        "attribute_value_list": [{"S": "1"}],
        "comparison_operator": "EQ"
    }
}

KEY_CONDITIONS_INDEX = {
    "hash_attr": {
        "attribute_value_list": [{"S": "1"}],
        "comparison_operator": "EQ"
    },
    "inumber": {
        "attribute_value_list": [{"N": "1"}],
        "comparison_operator": "EQ"
    }
}

SCAN_FILTER = {
    "hash_attr": {
        "attribute_value_list": [{"S": "1"}],
        "comparison_operator": "EQ"
    },
    "inumber": {
        "attribute_value_list": [{"N": "1"}],
        "comparison_operator": "EQ"
    }
}

ITEM = {
    "hash_attr": {"S": "1"},
    "range_attr": {"S": "1"},
    "inumber": {"N": "1"},
}

ATTRIBUTES_UPDATE = {
    "inumber": {
        "action": "PUT",
        "value": {"N": "1"},
    }
}

INDEX_NAME_N = "by_number"


class MagnetoDBTableOperationsTestCase(MagnetoDBTestCase):

    tenant_isolation = True

    def _check_exception(self, expected_exc, expected_message, method, *args,
                         **kwargs):
        with self.assertRaises(expected_exc) as r_exc:
            getattr(self.client, method)(*args, **kwargs)
        message = r_exc.exception._error_string
        self.assertIn(expected_message, message)

    def test_table_operations(self):
        tname = rand_name().replace('-', '')

        headers, body = self.client.list_tables()
        self.assertEqual(body, {'tables': []})

        not_found_msg = "'%s' does not exist" % tname
        self._check_exception(exceptions.NotFound, not_found_msg,
                              'delete_table', tname)
        self._check_exception(exceptions.NotFound, not_found_msg,
                              'get_item', tname, ITEM_PRIMARY_KEY)
        self._check_exception(exceptions.NotFound, not_found_msg,
                              'query', tname, key_conditions=KEY_CONDITIONS)
        self._check_exception(exceptions.NotFound, not_found_msg,
                              'query', tname, index_name=INDEX_NAME_N,
                              key_conditions=KEY_CONDITIONS_INDEX)
        self._check_exception(exceptions.NotFound, not_found_msg,
                              'scan', tname, scan_filter=SCAN_FILTER)
        self._check_exception(exceptions.NotFound, not_found_msg,
                              'put_item', tname, ITEM)
        self._check_exception(exceptions.NotFound, not_found_msg,
                              'update_item', tname, ITEM_PRIMARY_KEY,
                              ATTRIBUTES_UPDATE)
        self._check_exception(exceptions.NotFound, not_found_msg,
                              'delete_item', tname, ITEM_PRIMARY_KEY)
        self._check_exception(exceptions.NotFound, not_found_msg,
                              'describe_table', tname)

        headers, body = self.client.list_tables()
        self.assertEqual(body, {'tables': []})

        headers, body = self.client.create_table(
            ATTRIBUTE_DEFINITIONS,
            tname,
            KEY_SCHEMA,
            LSI_INDEXES)

        exc_message = 'Table %s already exists' % tname
        self._check_exception(exceptions.BadRequest, exc_message,
                              'create_table',
                              ATTRIBUTE_DEFINITIONS,
                              tname,
                              KEY_SCHEMA,
                              LSI_INDEXES)

        self.assertTrue(self.wait_for_table_active(tname))

        self._check_exception(exceptions.BadRequest, exc_message,
                              'create_table',
                              ATTRIBUTE_DEFINITIONS,
                              tname,
                              KEY_SCHEMA,
                              LSI_INDEXES)

        headers, body = self.client.list_tables()
        self.assertEqual(1, len(body['tables']))
        self.assertEqual(tname, body['tables'][0]['href'])

        self.client.put_item(tname, ITEM)
        self.client.get_item(tname, ITEM_PRIMARY_KEY)
        self.client.query(tname, key_conditions=KEY_CONDITIONS)
        self.client.query(tname, key_conditions=KEY_CONDITIONS_INDEX,
                          index_name=INDEX_NAME_N)
        self.client.scan(tname, scan_filter=SCAN_FILTER)
        self.client.update_item(tname, ITEM_PRIMARY_KEY, ATTRIBUTES_UPDATE)
        self.client.delete_item(tname, ITEM_PRIMARY_KEY)
        self.client.describe_table(tname)
        self.client.delete_table(tname)
        self.client.delete_table(tname)

        self.assertTrue(self.wait_for_table_deleted(tname))

        self._check_exception(exceptions.NotFound, not_found_msg,
                              'delete_table', tname)

        headers, body = self.client.list_tables()
        self.assertEqual(body, {'tables': []})

        # checking that data in the table is not accessible after table
        # deletion
        headers, body = self.client.create_table(
            ATTRIBUTE_DEFINITIONS,
            tname,
            KEY_SCHEMA,
            LSI_INDEXES)
        self.wait_for_table_active(tname)
        self.client.put_item(tname, ITEM)
        headers, body = self.client.query(tname, key_conditions=KEY_CONDITIONS)
        self.assertEqual(1, body['count'])

        self.client.delete_table(tname)
        self.wait_for_table_deleted(tname)

        headers, body = self.client.create_table(
            ATTRIBUTE_DEFINITIONS,
            tname,
            KEY_SCHEMA,
            LSI_INDEXES)
        self.wait_for_table_active(tname)
        headers, body = self.client.query(tname, key_conditions=KEY_CONDITIONS)
        self.assertEqual(0, body['count'])

        self.client.delete_table(tname)
        self.wait_for_table_deleted(tname)

#    def setUp(self):
#        super(MagnetoDBListTableTestCase, self).setUp()
#        self.tables = []
#
#    def tearDown(self):
#        for tname in self.tables:
#            try:
#                self.client.delete_table(tname)
#            except (exceptions.BadRequest, exceptions.NotFound):
#                self.tables.remove(tname)
#        while True:
#            for tname in self.tables:
#                try:
#                    self.client.describe_table(tname)
#                except exceptions.NotFound:
#                    self.tables.remove(tname)
#            if not self.tables:
#                break
#            time.sleep(1)
#        super(MagnetoDBListTableTestCase, self).tearDown()
#
#    @attr(type=['LisT-1'])
#    def test_list_tables_empty(self):
#        headers, body = self.client.list_tables()
#        expected = {'tables': []}
#        self.assertEqual(body, expected)
#
#    @attr(type=['LisT-2'])
#    def test_list_tables(self):
#        tname = rand_name().replace('-', '')
#        self._create_test_table(self.smoke_attrs, tname,
#                                self.smoke_schema,
#                                wait_for_active=True,
#                                cleanup=False)
#        self.tables.append(tname)
#        headers, body = self.client.list_tables()
#        expected = {'tables': [{'href': tname, 'rel': 'self'}]}
#        self.assertEqual(body, expected)
#
#    def _create_n_tables(self, num):
#        for i in range(0, num):
#            tname = rand_name().replace('-', '')
#            self._create_test_table(self.smoke_attrs, tname,
#                                    self.smoke_schema,
#                                    cleanup=False,
#                                    wait_for_active=True)
#            self.tables.append(tname)
#
#    @attr(type=['LisT-10'])
#    def test_list_tables_no_limit_5_tables(self):
#        tnum = 5
#        self._create_n_tables(tnum)
#        headers, body = self.client.list_tables()
#        self.assertEqual(len(body['tables']), tnum)
#
#    @attr(type=['LisT-12'])
#    def test_list_tables_limit_3_10_tables_with_exclusive(self):
#        tnum = 10
#        limit = 3
#        self._create_n_tables(tnum)
#        last_evaluated_table_name = None
#        for i in range(0, tnum / limit):
#            headers, body = self.client.list_tables(
#                limit=limit,
#                exclusive_start_table_name=last_evaluated_table_name)
#            last_evaluated_table_name = body['last_evaluated_table_name']
#            self.assertEqual(len(body['tables']), limit)
#            self.assertEqual(body['tables'][-1]['href'],
#                             last_evaluated_table_name)
#        headers, body = self.client.list_tables(
#            limit=limit,
#            exclusive_start_table_name=last_evaluated_table_name)
#        self.assertEqual(len(body['tables']), tnum % limit)
#
#    @attr(type=['LisT-30'])
#    def test_list_tables_no_exclusive(self):
#        tnum = 5
#        self._create_n_tables(tnum)
#        headers, body = self.client.list_tables()
#        self.assertEqual(len(body['tables']), tnum)
#
#    @attr(type=['LisT-31', 'LisT-33'])
#    def test_list_tables_exclusive(self):
#        tnum = 5
#        limit = 3
#        self._create_n_tables(tnum)
#        headers, body = self.client.list_tables(limit=limit)
#        last_evaluated_table_name = body['last_evaluated_table_name']
#        headers, body = self.client.list_tables(
#            limit=limit,
#            exclusive_start_table_name=last_evaluated_table_name)
#        self.assertEqual(len(body['tables']), tnum % limit)
#
#    @attr(type=['LisT-32'])
#    def test_list_tables_exclusive_no_previous_run(self):
#        tnum = 5
#        self._create_n_tables(tnum)
#        self.tables.sort()
#        last_evaluated_table_name = self.tables[0]
#        headers, body = self.client.list_tables(
#            exclusive_start_table_name=last_evaluated_table_name)
#        self.assertEqual(len(body['tables']), tnum - 1)
#
#    @attr(type=['LisT-34'])
#    def test_list_tables_exclusive_3_symb(self):
#        tnames = 'aaa', 'bbb'
#        for tname in tnames:
#            self._create_test_table(self.smoke_attrs, tname,
#                                    self.smoke_schema,
#                                    cleanup=False,
#                                    wait_for_active=True)
#            self.tables.append(tname)
#        last_evaluated_table_name = self.tables[0]
#        headers, body = self.client.list_tables(
#            exclusive_start_table_name=last_evaluated_table_name)
#        self.assertEqual(len(body['tables']), 1)
#
#    @attr(type=['LisT-38'])
#    def test_list_tables_exclusive_non_existent(self):
#        tnames = 'aaa1', 'bbb'
#        for tname in tnames:
#            self._create_test_table(self.smoke_attrs, tname,
#                                    self.smoke_schema,
#                                    cleanup=False,
#                                    wait_for_active=True)
#            self.tables.append(tname)
#        last_evaluated_table_name = 'aaa'
#        headers, body = self.client.list_tables(
#            exclusive_start_table_name=last_evaluated_table_name)
#        self.assertEqual(len(body['tables']), 2)
