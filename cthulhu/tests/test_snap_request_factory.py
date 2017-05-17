from unittest import TestCase
from mock import MagicMock, patch

from cthulhu.manager.snap_request_factory import SnapRequestFactory
from cthulhu.manager.user_request import RadosRequest
import json


class TestSnapFactory(TestCase):

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

        self.factory = SnapRequestFactory(fake_cluster_monitor)

    @patch('cthulhu.manager.user_request.LocalClient', fake_salt)
    def test_create_snap(self):
        attrs = {
                  'name': 'test',
                  'pool_name': 'test',
                  'snap_name': 'snap'
                }
        created = self.factory.create(attrs)
        created.submit(54321)
        assert self.fake_salt.run_job.call_args[0][2][2] == [('create_snapshot', attrs)]

    @patch('cthulhu.manager.user_request.LocalClient', fake_salt)
    def test_update_snap(self):
        attrs = {
                  'name': 'test',
                  'pool_name': 'test',
                  'protected': False,
                  'snap_name': 'snap',
                  'new_snap_name': 'new_snap'
                }
        
        updated = self.factory.update('test', attrs)
        updated.submit(54321)
        assert self.fake_salt.run_job.call_args[0][2][2] == \
            [('unprotect_snapshot', attrs), ('rename_snapshot', attrs)]

    @patch('cthulhu.manager.user_request.LocalClient', fake_salt)
    def test_delete_snap(self):
        attrs = {
                  'name': 'test',
                  'pool_name': 'test',
                  'snap_name': 'snap'
                }
        deleted = self.factory.delete(attrs['name'], attrs)
        deleted.submit(54321)
        assert self.fake_salt.run_job.call_args[0][2][2] == [('remove_snapshot', attrs)]

