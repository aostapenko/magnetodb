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

import base64

from tempest.api.keyvalue.rest_base.base import MagnetoDBTestCase
from tempest.common.utils.data_utils import rand_name
from tempest.test import attr


class MagnetoDBBatchGetTest(MagnetoDBTestCase):

    def setUp(self):
        super(MagnetoDBBatchGetTest, self).setUp()
        self.tname = rand_name().replace('-', '')

    @attr(type=['BGI-1'])
    def test_batch_get_mandatory(self):
        self._create_test_table(self.build_x_attrs('S'),
                                self.tname,
                                self.smoke_schema,
                                wait_for_active=True)
        item = self.build_x_item('S', 'forum1', 'subject2',
                                 ('message', 'S', 'message text'))
        self.client.put_item(self.tname, item)
        key = {self.hashkey: {'S': 'forum1'}, self.rangekey: {'S': 'subject2'}}
        request_body = {'request_items': {self.tname: {'keys': [key]}}}
        headers, body = self.client.batch_get_item(request_body)
        self.assertIn('unprocessed_items', body)
        self.assertIn('responses', body)
        expected_responses = {self.tname: [item]}
        self.assertEqual({}, body['unprocessed_items'])
        self.assertEqual(expected_responses, body['responses'])

    @attr(type=['BGI-2'])
    def test_batch_get_all_params(self):
        self._create_test_table(self.build_x_attrs('S'),
                                self.tname,
                                self.smoke_schema,
                                wait_for_active=True)
        item = self.build_x_item('S', 'forum1', 'subject2',
                                 ('message', 'S', 'message text'))
        self.client.put_item(self.tname, item)
        key = {self.hashkey: {'S': 'forum1'}, self.rangekey: {'S': 'subject2'}}
        attr_to_get = ['forum']
        request_body = {
            'request_items':
            {
                self.tname:
                {
                    'keys': [key],
                    'attributes_to_get': attr_to_get,
                    'consistent_read': True
                }
            }
        }
        headers, body = self.client.batch_get_item(request_body)
        self.assertIn('unprocessed_items', body)
        self.assertIn('responses', body)
        expected_responses = {self.tname: [{'forum': {'S': 'forum1'}}]}
        self.assertEqual({}, body['unprocessed_items'])
        self.assertEqual(expected_responses, body['responses'])

    @attr(type=['BGI-3'])
    def test_batch_get_one_table(self):
        self._create_test_table(self.build_x_attrs('S'),
                                self.tname,
                                self.smoke_schema,
                                wait_for_active=True)
        item = self.build_x_item('S', 'forum1', 'subject2',
                                 ('message', 'S', 'message text'))
        self.client.put_item(self.tname, item)
        key = {self.hashkey: {'S': 'forum1'}, self.rangekey: {'S': 'subject2'}}
        request_body = {'request_items': {self.tname: {'keys': [key]}}}
        headers, body = self.client.batch_get_item(request_body)
        self.assertIn('responses', body)
        self.assertIn(self.tname, body['responses'])

    @attr(type=['BGI-4'])
    def test_batch_get_several_tables(self):
        tables = []
        item = self.build_x_item('S', 'forum1', 'subject2',
                                 ('message', 'S', 'message text'))
        for i in range(0, 3):
            tname = rand_name().replace('-', '')
            tables.append(tname)
            self._create_test_table(self.build_x_attrs('S'),
                                    tname,
                                    self.smoke_schema,
                                    wait_for_active=True)
            self.client.put_item(tname, item)

        key = {self.hashkey: {'S': 'forum1'}, self.rangekey: {'S': 'subject2'}}
        request_body = {'request_items': {tname:
            {'keys': [key]} for tname in tables}}
        headers, body = self.client.batch_get_item(request_body)
        self.assertIn('responses', body)
        self.assertEqual(set(tables), set(body['responses'].keys()))
