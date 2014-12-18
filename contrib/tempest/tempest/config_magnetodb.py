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

from __future__ import print_function

from oslo.config import cfg

from tempest import config  # noqa


magnetodb_group = cfg.OptGroup(name="magnetodb",
                               title="Key-Value storage options")

MagnetoDBGroup = [
    cfg.StrOpt('service_type',
               default="kv-storage",
               help="The name of the MagnetoDB service type"),
    cfg.BoolOpt('allow_tenant_isolation',
                default=False,
                help="Allows test cases to create/destroy tenants and "
                     "users. This option enables isolated test cases and "
                     "better parallel execution, but also requires that "
                     "OpenStack Identity API admin credentials are known."),
]

magnetodb_streaming_group = cfg.OptGroup(
    name="magnetodb_streaming",
    title="Key-Value storage steaming API options")

MagnetoDBStreamingGroup = [
    cfg.StrOpt('service_type',
               default="kv-streaming",
               help="The name of the MagnetoDB streaming API service type"),
]

magnetodb_monitoring_group = cfg.OptGroup(
    name="magnetodb_monitoring",
    title="Key-Value storage monitoring API options")

MagnetoDBMonitoringGroup = [
    cfg.StrOpt('service_type',
               default="kv-monitoring",
               help="The name of the MagnetoDB monitoring API service type"),
]

magnetodb_management_group = cfg.OptGroup(
    name="magnetodb_management",
    title="Key-Value storage management API options")

MagnetoDBManagementGroup = [
    cfg.StrOpt('service_type',
               default="kv-management",
               help="The name of the MagnetoDB management API service type"),
]


class TempestConfigPrivateMagnetoDB(config.TempestConfigPrivate):

    # manila's config wrap over standard config
    def __init__(self, parse_conf=True):
        super(TempestConfigPrivateManila, self).__init__()
        config.register_opt_group(cfg.CONF, magnetodb_group, MagnetoDBGroup)
        config.register_opt_group(cfg.CONF, magnetodb_streaming_group,
                                  MagnetoDBStreamingGroup)
        config.register_opt_group(cfg.CONF, magnetodb_management_group,
                                  MagnetoDBManagementGroup)
        config.register_opt_group(cfg.CONF, magnetodb_monitoring_group,
                                  MagnetoDBMonitoringGroup)
        self.magnetodb = cfg.CONF.magnetodb


class TempestConfigProxyMagnetoDB(object):                                                                                                                                  
    _config = None                                                                                                                                                       

    def __getattr__(self, attr):                                                                                                                                         
        if not self._config:                                                                                                                                             
            self._config = TempestConfigPrivateMagnetoDB()                                                                                                                  
        return getattr(self._config, attr)                                                                                                                               


CONF = TempestConfigProxyMagnetoDB()
