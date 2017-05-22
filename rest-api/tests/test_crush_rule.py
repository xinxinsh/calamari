import mock
import logging

from django.utils.unittest import TestCase
from calamari_rest.views.v2 import CrushRuleViewSet
from calamari_rest.views.v2 import OSD_MAP, CRUSH_RULE

log = logging.getLogger(__name__)

def fake_sync_object(fsid, obj_type, filters=None):
    if obj_type == OSD_MAP:
        return '{0: [0, 3], 1: [2, 5], 2: [1, 4]}'
    else:
        raise NotImplementedError()

def fake_get(fsid, obj_type, obj_id):
    ruleset = [
	{
	    "min_size": 1, 
	    "rule_name": "hdd", 
	    "steps": [
		{
		    "item_name": "hdd", 
		    "item": -5, 
		    "op": "take"
		}, 
		{
		    "num": 0, 
		    "type": "host", 
		    "op": "chooseleaf_firstn"
		}, 
		{
		    "op": "emit"
		}
	    ], 
	    "ruleset": 0, 
	    "type": 1, 
	    "rule_id": 0, 
	    "max_size": 10
	}, 
	{
	    "min_size": 1, 
	    "rule_name": "ssd", 
	    "steps": [
		{
		    "item_name": "ssd", 
		    "item": -7, 
		    "op": "take"
		}, 
		{
		    "num": 0, 
		    "type": "host", 
		    "op": "chooseleaf_firstn"
		}, 
		{
		    "op": "emit"
		}
	    ], 
	    "ruleset": 2, 
	    "type": 1, 
	    "rule_id": 1, 
	    "max_size": 10
	}, 
	{
	    "min_size": 1, 
	    "rule_name": "edd", 
	    "steps": [
		{
		    "item_name": "edd", 
		    "item": -2, 
		    "op": "take"
		}, 
		{
		    "num": 0, 
		    "type": "host", 
		    "op": "chooseleaf_firstn"
		}, 
		{
		    "op": "emit"
		}
	    ], 
	    "ruleset": 3, 
	    "type": 1, 
	    "rule_id": 2, 
	    "max_size": 6
	}
	]

    if obj_type == CRUSH_RULE and obj_id is not None:
        return ruleset[obj_id]
    elif obj_id is None:
        return ruleset
    else:
        raise NotImplementedError()

class TestCrushRule(TestCase):

    def setUp(self):
        self.request = mock.Mock()

        with mock.patch('calamari_rest.views.v2.RPCViewSet'):
            self.cmvs = CrushRuleViewSet()
            self.cmvs.client = mock.MagicMock()
            self.cmvs.client.get = mock.MagicMock(side_effect=fake_get)
            self.cmvs.client.get_sync_object = mock.MagicMock(side_effect=fake_sync_object)
            self.cmvs.client.update.return_value = ["request_id"]
            self.cmvs.client.delete.return_value = ["request_id"]
            self.cmvs.client.create.return_value = ["request_id"]

    def test_list(self):
        self.request.method = 'GET'
        response = self.cmvs.list(self.request, 12345)
        self.assertEqual(response.status_code, 200)
 
    def test_retrieve(self):
        self.request.method = 'GET'
        response = self.cmvs.retrieve(self.request, 12345, 2)
        self.assertEqual(response.status_code, 200)

    def test_update(self):
        self.request.method = 'PUT'
        self.request.DATA = {
            'name': 'test', 
            'ruleset': 3, 
            'min_size': 1, 
            'max_size': 10, 
            'steps': [
            {
                "item_name": "hdd", 
                "item": -5, 
                "op": "take"
            }, 
            {
                "num": 0, 
                "type": "host", 
                "op": "chooseleaf_firstn"
            }, 
            {
                "op": "emit"
            }
        ]}
        response = self.cmvs.update(self.request, 12345, 2)
        self.assertEqual(response.status_code, 202)

    def test_delete(self):
        self.request.method = 'DELETE'
        response = self.cmvs.destroy(self.request, 12345, 2)
        self.assertEqual(response.status_code, 202)

    def test_create(self):
        self.request.method = 'POST'
        self.request.DATA = {
            'name': 'test',
            "type": "replicated",
            'ruleset': 3,
            'min_size': 1,
            'max_size': 10,
            'steps': [
            {
                "item_name": "hdd",
                "item": -5,
                "op": "take"
            },
            {
                "num": 0,
                "type": "host",
                "op": "chooseleaf_firstn"
            },
            {
                "op": "emit"
            }
        ]}
        response = self.cmvs.create(self.request, 12345)
        self.assertEqual(response.status_code, 202)
