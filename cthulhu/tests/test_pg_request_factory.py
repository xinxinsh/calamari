from django.utils.unittest import TestCase
from mock import MagicMock, patch

from cthulhu.manager.pg_request_factory import PgRequestFactory
from cthulhu.manager.user_request import RadosRequest
import json

class TestPgFactory(TestCase):

    salt_local_client = MagicMock(run_job=MagicMock())
    salt_local_client.return_value = salt_local_client
    salt_local_client.run_job.return_value = {'jid': 12345}

    def setUp(self):
        fake_cluster_monitor = MagicMock()
        attributes = {'name': 'I am a fake',
                      'fsid': 12345,
                      'get_sync_object.return_value': fake_cluster_monitor
                      }
        fake_cluster_monitor.configure_mock(**attributes)

        self.pg_request_factory = PgRequestFactory(fake_cluster_monitor)


    @patch('cthulhu.manager.user_request.LocalClient', salt_local_client)
    def test_scrub(self):
        scrub = self.pg_request_factory.scrub('1.a')
        self.assertIsInstance(scrub, RadosRequest, 'Testing Scrub')

        scrub.submit(54321)
        self.salt_local_client.run_job.assert_called_with(54321, 'ceph.rados_commands', [12345, 'I am a fake', [('pg scrub', {'pgid': '1.a'})]])

    @patch('cthulhu.manager.user_request.LocalClient', salt_local_client)
    def test_deepscrub(self):
        deep_scrub = self.pg_request_factory.deepscrub('1.a')
        self.assertIsInstance(deep_scrub, RadosRequest, 'Testing DeepScrub')

        deep_scrub.submit(54321)
        self.salt_local_client.run_job.assert_called_with(54321, 'ceph.rados_commands', [12345, 'I am a fake', [('pg deep-scrub', {'pgid': '1.a'})]])

    @patch('cthulhu.manager.user_request.LocalClient', salt_local_client)
    def test_repair(self):
        repair = self.pg_request_factory.repair('1.a')
        self.assertIsInstance(repair, RadosRequest, 'Testing Repair')

        repair.submit(54321)
        self.salt_local_client.run_job.assert_called_with(54321, 'ceph.rados_commands', [12345, 'I am a fake', [('pg repair', {'pgid': '1.a'})]])


