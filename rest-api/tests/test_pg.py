import mock
import logging

from django.utils.unittest import TestCase
from calamari_rest.views.v2 import PgViewSet
from calamari_rest.views.v2 import POOL

log = logging.getLogger(__name__)

def fake_sync_object(fsid, obj_type, filters=None):
    if obj_type == 'pg_summary' and filters == 'details':
        return [
        {
            "acting_primary": 3, 
            "up_primary": 3, 
            "pgid": "3.9e", 
            "state": "active+clean", 
            "up": [
                3, 
                0
            ], 
            "acting": [
                3, 
                0
            ]
        }, 
        {
            "acting_primary": 0, 
            "up_primary": 0, 
            "pgid": "2.9f", 
            "state": "active+clean", 
            "up": [
                0, 
                3
            ], 
            "acting": [
                0, 
                3
            ]
        }, 
        {
            "acting_primary": 0, 
            "up_primary": 0, 
            "pgid": "3.9f", 
            "state": "active+clean", 
            "up": [
                0, 
                3
            ], 
            "acting": [
                0, 
                3
            ]
        }, 
        {
            "acting_primary": 3, 
            "up_primary": 3, 
            "pgid": "2.9e", 
            "state": "active+clean", 
            "up": [
                3, 
                0
            ], 
            "acting": [
                3, 
                0
            ]
        }, 
        {
            "acting_primary": 0, 
            "up_primary": 0, 
            "pgid": "3.9c", 
            "state": "active+clean", 
            "up": [
                0, 
                3
            ], 
            "acting": [
                0, 
                3
            ]
        }]
    elif obj_type == 'pg_summary' and filters == ['pg_by_id']:
        return {
        "3.9e": {
            "acting_primary": 3, 
            "up_primary": 3, 
            "pgid": "3.9e", 
            "state": "active+clean", 
            "up": [
                3, 
                0
            ], 
            "acting": [
                3, 
                0
            ]
        }, 
        "2.9f": {
            "acting_primary": 0, 
            "up_primary": 0, 
            "pgid": "2.9f", 
            "state": "active+clean", 
            "up": [
                0, 
                3
            ], 
            "acting": [
                0, 
                3
            ]
        }, 
        "3.9f": {
            "acting_primary": 0, 
            "up_primary": 0, 
            "pgid": "3.9f", 
            "state": "active+clean", 
            "up": [
                0, 
                3
            ], 
            "acting": [
                0, 
                3
            ]
        }, 
        "2.9e": {
            "acting_primary": 3, 
            "up_primary": 3, 
            "pgid": "2.9e", 
            "state": "active+clean", 
            "up": [
                3, 
                0
            ], 
            "acting": [
                3, 
                0
            ]
        }, 
        "3.9c": {
            "acting_primary": 0, 
            "up_primary": 0, 
            "pgid": "3.9c", 
            "state": "active+clean", 
            "up": [
                0, 
                3
            ], 
            "acting": [
                0, 
                3
            ]
        }}

class TestPg(TestCase):

    def setUp(self):
        self.request = mock.Mock()

        with mock.patch('calamari_rest.views.v2.RPCViewSet'):
            self.cmvs = PgViewSet()
            self.cmvs.client = mock.MagicMock()
            self.cmvs.client.get_sync_object = mock.MagicMock(side_effect=fake_sync_object)
            self.cmvs.client.get_valid_commands.return_value = {'valid_commands': ['scrub', 'deep_scrub', 'repair']}
            self.cmvs.client.apply.return_value = ['request_id']

    def test_list(self):
        self.request.method = 'GET'
        response = self.cmvs.list(self.request, 12345)
        self.assertEqual(response.status_code, 200)
        
    def test_retrieve(self):
        pg = "3.9c"
        self.request.method = 'GET'
        response = self.cmvs.retrieve(self.request, 12345, pg)
        self.assertEqual(response.status_code, 200)
        
    def test_apply(self):
        """
        That we can apply ceph commands to an PG
        """
        self.request.method = 'POST'
        commands = ['scrub', 'deep_scrub', 'repair']
        pg = '3.9c'

        for x in commands:
            response = self.cmvs.apply(self.request, 12345, pg, x)
            self.assertEqual(response.status_code, 202)
