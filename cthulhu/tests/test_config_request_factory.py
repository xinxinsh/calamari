from unittest import TestCase
from mock import MagicMock, patch

from cthulhu.manager.config_request_factory import ConfigRequestFactory
from cthulhu.manager.user_request import RadosRequest
import json


class TestConfigFactory(TestCase):

    fake_salt = MagicMock(run_job=MagicMock())
    fake_salt.return_value = fake_salt
    fake_salt.run_job.return_value = {'jid': 12345}

    def setUp(self):
        fake_osd_map = MagicMock()

        fake_cluster_monitor = MagicMock()
        attributes = {'name': 'I am a fake',
                      'fsid': 12345,
                      'get_sync_object.return_value': fake_osd_map}
        fake_cluster_monitor.configure_mock(**attributes)

        self.factory = ConfigRequestFactory(fake_cluster_monitor)

    @patch('cthulhu.manager.user_request.LocalClient', fake_salt)
    def test_update_config(self):
        attrs = {
                  'targets': [('osd', 0),('mon', 'master')],
                  'key': 'rbd_cache',
                  'value': True
                }
        
        updated = self.factory.update('rbd_cache', attrs)
        updated.submit(54321)
        assert self.fake_salt.run_job.call_args[0][2][2] == \
            [('injectargs', {'data': {'args': {'injected_args': ['--rbd_cache', True]}, 'targets': [('osd', 0), ('mon', 'master')]}})]
