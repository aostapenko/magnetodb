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

ITEM_UPDATE = {
    "hash_attr": {
        "action": "PUT",
        "value": {"S": "1"},
    },
    "range_attr": {
        "action": "PUT",
        "value": {"S": "1"},
    }
}

INDEX_NAME_N = "by_number"

OPERATIONS_DICT = {
    "delete_table": {
        "method": "delete_table",
        "kwargs": {
            "table_name": TABLE_NAME
        }
    },
    "get_item": {
        "method": "get_item",
        "kwargs": {
            "key": ITEM_PRIMARY_KEY,
            "table_name": TABLE_NAME
        }
    },
    "query": {
        "method": "query",
        "kwargs": {
            "key_conditions": KEY_CONDITIONS,
            "table_name": TABLE_NAME
        }
    },
    "query_by_index": {
        "method": "query",
        "kwargs": {
            "key_conditions": KEY_CONDITIONS_INDEX,
            "index_name": INDEX_NAME_N,
            "table_name": TABLE_NAME
        }
    },
    "scan": {
        "method": "scan",
        "kwargs": {
            "scan_filter": SCAN_FILTER,
            "table_name": TABLE_NAME
        }
    },
    "put_item": {
        "method": "put_item",
        "kwargs": {
            "item": ITEM,
            "table_name": TABLE_NAME
        }
    },
    "update_item": {
        "method": "update_item",
        "kwargs": {
            "key": ITEM_PRIMARY_KEY,
            "attribute_updates": ITEM_UPDATE,
            "table_name": TABLE_NAME
        }
    },
    "delete_item": {
        "method": "delete_item",
        "kwargs": {
            "key": ITEM_PRIMARY_KEY,
            "table_name": TABLE_NAME
        }
    }
}


class MagnetoDBTableOperationsTestCase(MagnetoDBTestCase):

    tenant_isolation = True

    def test_operations_with_nonexistent_table(self):
        headers, body = self.client.list_tables()
        self.assertEqual(body, {'tables': []})

        operations = ["delete_table", "get_item", "query", "query_by_index",
                      "scan", "put_item", "update_item", "delete_item"]
        for op in operations:
            kwargs = OPERATIONS_DICT[op].get('kwargs', {})
            with self.assertRaises(exceptions.NotFound) as r_exc:
                (getattr(self.client, OPERATIONS_DICT[op]['method'])(**kwargs))
            message = r_exc.exception._error_string
            self.assertIn("'%s' does not exist" % TABLE_NAME, message)

        headers, body = self.client.list_tables()
        self.assertEqual(body, {'tables': []})

        headers, body = self.client.create_table(
            ATTRIBUTE_DEFINITIONS,
            TABLE_NAME,
            KEY_SCHEMA,
            LSI_INDEXES)

        with self.assertRaises(exceptions.BadRequest) as r_exc:
            headers, body = self.client.create_table(
                ATTRIBUTE_DEFINITIONS,
                TABLE_NAME,
                KEY_SCHEMA,
                LSI_INDEXES)

        self.wait_for_table_active(TABLE_NAME)

        with self.assertRaises(exceptions.BadRequest) as r_exc:
            headers, body = self.client.create_table(
                ATTRIBUTE_DEFINITIONS,
                TABLE_NAME,
                KEY_SCHEMA,
                LSI_INDEXES)

        headers, body = self.client.list_tables()
        self.assertEqual(1, len(body['tables']))
        self.assertEqual(TABLE_NAME, body['tables'][0]['href'])

        for op in operations:
            kwargs = OPERATIONS_DICT[op].get('kwargs', {})
            getattr(self.client, OPERATIONS_DICT[op]['method'])(**kwargs)

        

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
