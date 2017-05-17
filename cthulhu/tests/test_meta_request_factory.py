from unittest import TestCase
from mock import MagicMock, patch

from cthulhu.manager.meta_request_factory import MetaRequestFactory
from cthulhu.manager.user_request import RadosRequest
import json


class TestMetaFactory(TestCase):

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

        self.factory = MetaRequestFactory(fake_cluster_monitor)

    @patch('cthulhu.manager.user_request.LocalClient', fake_salt)
    def test_create_meta(self):
        attrs = {
                  'conf_rbd_cache': False,
                  'name': 'test',
                  'pool_name': 'test'
                }
        created = self.factory.create(attrs)
        created.submit(54321)
        assert self.fake_salt.run_job.call_args[0][2][2] == [('set_metadata', attrs)]

    @patch('cthulhu.manager.user_request.LocalClient', fake_salt)
    def test_update_meta(self):
        attrs = {
                  'conf_rbd_cache': False,
                  'name': 'test',
                  'pool_name': 'test'
                }
        updated = self.factory.update('test', attrs)
        updated.submit(54321)
        assert self.fake_salt.run_job.call_args[0][2][2] == \
            [('set_metadata', attrs)]

    @patch('cthulhu.manager.user_request.LocalClient', fake_salt)
    def test_delete_meta(self):
        attrs = {
                  'conf_rbd_cache': False,
                  'name': 'test',
                  'pool_name': 'test'
                }
        deleted = self.factory.delete('test', attrs)
        deleted.submit(54321)
        assert self.fake_salt.run_job.call_args[0][2][2] == [('remove_metadata', attrs)]

