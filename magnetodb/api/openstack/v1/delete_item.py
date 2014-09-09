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

import jsonschema

from magnetodb import storage
from magnetodb.api.openstack.v1 import parser
from magnetodb.api.openstack.v1 import utils
from magnetodb.common import probe


class DeleteItemController(object):
    """ Deletes a single item in a table by primary key. """

    schema = {
        "required": [parser.Props.KEY],
        "properties": {
            parser.Props.EXPECTED: {
                "type": "object",
                "patternProperties": {
                    parser.ATTRIBUTE_NAME_PATTERN: {
                        "oneOf": [
                            {
                                "type": "object",
                                "required": [parser.Props.EXISTS],
                                "properties": {
                                    parser.Props.EXISTS: {
                                        "type": "boolean",
                                    },
                                }
                            },
                            {
                                "type": "object",
                                "required": [parser.Props.VALUE],
                                "properties": {
                                    parser.Props.VALUE:
                                        parser.Types.TYPED_ATTRIBUTE_VALUE,
                                }
                            },
                        ]
                    }
                }
            },

            parser.Props.KEY: parser.Types.ITEM,

            parser.Props.RETURN_VALUES: {
                "type": "string",
                "enum": [parser.Values.RETURN_VALUES_NONE,
                         parser.Values.RETURN_VALUES_ALL_OLD]
            },
            parser.Props.TABLE_NAME: parser.Types.TABLE_NAME
        }
    }

    @probe.Probe(__name__)
    def process_request(self, req, body, project_id, table_name):
        with probe.Probe(__name__ + '.jsonschema.validate'):
            jsonschema.validate(body, self.schema)
        utils.check_project_id(req.context, project_id)

        # parse expected item conditions
        expected_item_conditions = (
            parser.Parser.parse_expected_attribute_conditions(
                body.get(parser.Props.EXPECTED, {})
            )
        )

        # parse key_attributes
        key_attributes = parser.Parser.parse_item_attributes(
            body.get(parser.Props.KEY, {})
        )

        # parse return_values param
        return_values = body.get(
            parser.Props.RETURN_VALUES, parser.Values.RETURN_VALUES_NONE
        )

        # delete item
        req.context.tenant = project_id
        storage.delete_item(req.context, table_name, key_attributes,
                            expected_condition_map=expected_item_conditions)

        # format response
        response = {}

        if return_values != parser.Values.RETURN_VALUES_NONE:
            # TODO(cwang):
            # It is needed to return all deleted item attributes
            #
            response[parser.Props.ATTRIBUTES] = (
                parser.Parser.format_item_attributes(key_attributes)
            )

        return response
