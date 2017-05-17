from unittest import TestCase
from mock import MagicMock, patch

from cthulhu.manager.rbd_request_factory import RbdRequestFactory
from cthulhu.manager.user_request import RadosRequest
import json


class TestRbdFactory(TestCase):

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

        self.factory = RbdRequestFactory(fake_cluster_monitor)

    @patch('cthulhu.manager.user_request.LocalClient', fake_salt)
    def test_create_rbd(self):
        attrs = {
                  'name': 'test',
                  'pool_name': 'test',
                  'size': 1024000000,
                  'order': 22
                }
        created = self.factory.create(attrs)
        created.submit(54321)
        assert self.fake_salt.run_job.call_args[0][2][2] == [('create_image', attrs)]

    @patch('cthulhu.manager.user_request.LocalClient', fake_salt)
    def test_update_rbd(self):
        attrs = {
                  'name': 'test',
                  'new_name': 'rename_test',
                  'pool_name': 'test',
                  'size': 2024000000,
                  'features': 61
                }
        updated = self.factory.update('test', attrs)
        updated.submit(54321)
        assert self.fake_salt.run_job.call_args[0][2][2] == \
            [('resize_image', attrs), ('update_features', attrs), ('rename_image', attrs)]

    @patch('cthulhu.manager.user_request.LocalClient', fake_salt)
    def test_delete_rbd(self):
        attrs = {
                  'name': 'test',
                  'pool_name': 'test'
                }
        deleted = self.factory.delete(attrs['name'], attrs)
        deleted.submit(54321)
        assert self.fake_salt.run_job.call_args[0][2][2] == [('remove_image', attrs)]

