import mock
import logging

from django.utils.unittest import TestCase
from calamari_rest.views.v2 import RbdViewSet
from calamari_rest.views.v2 import POOL

log = logging.getLogger(__name__)

def fake_sync_object(fsid, obj_type, filters=None):
    if obj_type == 'rbd_summary' and filters is None:
        return {
	    "test": {
		"test": {
		    "parent_name": "", 
		    "parent_pool": 18446744073709551615, 
		    "features": 61, 
		    "lockers": [], 
		    "num_objs": 2560, 
		    "block_name_prefix": "rbd_data.26a9a6b8b4567", 
		    "old_format": False, 
		    "snaps": {
			"snap": {
			    "size": 10737418240, 
			    "protected": True, 
			    "id": 4, 
			    "name": "snap", 
			    "children": [
				{
				    "name": "clone", 
				    "pool": "images"
				}, 
				{
				    "name": "clone1", 
				    "pool": "images"
				}
			    ]
			}, 
			"snap1": {
			    "size": 10737418240, 
			    "protected": False, 
			    "id": 5, 
			    "name": "snap1", 
			    "children": []
			}
		    }, 
		    "obj_size": 4194304, 
		    "meta": {
			"conf_rbd_non_blocking_aio": "False"
		    }, 
		    "parent_info": [], 
		    "flags": 0, 
		    "size": 10737418240, 
		    "order": 22, 
		    "name": "test"
		}, 
		"test1": {
		    "parent_name": "", 
		    "parent_pool": 18446744073709551615, 
		    "features": 61, 
		    "lockers": [], 
		    "num_objs": 2560, 
		    "block_name_prefix": "rbd_data.26a9a327b23c6", 
		    "old_format": False, 
		    "snaps": {}, 
		    "obj_size": 4194304, 
		    "meta": {}, 
		    "parent_info": [], 
		    "flags": 0, 
		    "size": 10737418240, 
		    "order": 22, 
		    "name": "test1"
		}, 
		"clone": {
		    "parent_name": "", 
		    "parent_pool": 18446744073709551615, 
		    "features": 61, 
		    "lockers": [], 
		    "num_objs": 2560, 
		    "block_name_prefix": "rbd_data.5f4ba6b8b4567", 
		    "old_format": False, 
		    "snaps": {}, 
		    "obj_size": 4194304, 
		    "meta": {}, 
		    "parent_info": [
			"images", 
			"test", 
			"snap"
		    ], 
		    "flags": 0, 
		    "size": 10737418240, 
		    "order": 22, 
		    "name": "clone"
		}, 
		"test2": {
		    "parent_name": "", 
		    "parent_pool": 18446744073709551615, 
		    "features": 61, 
		    "lockers": [], 
		    "num_objs": 2560, 
		    "block_name_prefix": "rbd_data.26a9a643c9869", 
		    "old_format": False, 
		    "snaps": {}, 
		    "obj_size": 4194304, 
		    "meta": {}, 
		    "parent_info": [], 
		    "flags": 0, 
		    "size": 10737418240, 
		    "order": 22, 
		    "name": "test2"
		}
	    }, 
	    "test1": {}
	}

    elif obj_type == 'rbd_summary' and filters is not None:
		return {
	    "test": [
		 {
		    "parent_name": "", 
		    "parent_pool": 18446744073709551615, 
		    "features": 61, 
		    "lockers": [], 
		    "num_objs": 2560, 
		    "block_name_prefix": "rbd_data.26a9a6b8b4567", 
		    "old_format": False, 
		    "snaps": {
			"snap": {
			    "size": 10737418240, 
			    "protected": True, 
			    "id": 4, 
			    "name": "snap", 
			    "children": [
				{
				    "name": "clone", 
				    "pool": "images"
				}, 
				{
				    "name": "clone1", 
				    "pool": "images"
				}
			    ]
			}, 
			"snap1": {
			    "size": 10737418240, 
			    "protected": False, 
			    "id": 5, 
			    "name": "snap1", 
			    "children": []
			}
		    }, 
		    "obj_size": 4194304, 
		    "meta": {
			"conf_rbd_non_blocking_aio": "False"
		    }, 
		    "parent_info": [], 
		    "flags": 0, 
		    "size": 10737418240, 
		    "order": 22, 
		    "name": "test"
		}, 
		{
		    "parent_name": "", 
		    "parent_pool": 18446744073709551615, 
		    "features": 61, 
		    "lockers": [], 
		    "num_objs": 2560, 
		    "block_name_prefix": "rbd_data.26a9a327b23c6", 
		    "old_format": False, 
		    "snaps": {}, 
		    "obj_size": 4194304, 
		    "meta": {}, 
		    "parent_info": [], 
		    "flags": 0, 
		    "size": 10737418240, 
		    "order": 22, 
		    "name": "test1"
		}, 
		{
		    "parent_name": "", 
		    "parent_pool": 18446744073709551615, 
		    "features": 61, 
		    "lockers": [], 
		    "num_objs": 2560, 
		    "block_name_prefix": "rbd_data.5f4ba6b8b4567", 
		    "old_format": False, 
		    "snaps": {}, 
		    "obj_size": 4194304, 
		    "meta": {}, 
		    "parent_info": [
			"images", 
			"test", 
			"snap"
		    ], 
		    "flags": 0, 
		    "size": 10737418240, 
		    "order": 22, 
		    "name": "clone"
		}, 
		{
		    "parent_name": "", 
		    "parent_pool": 18446744073709551615, 
		    "features": 61, 
		    "lockers": [], 
		    "num_objs": 2560, 
		    "block_name_prefix": "rbd_data.26a9a643c9869", 
		    "old_format": False, 
		    "snaps": {}, 
		    "obj_size": 4194304, 
		    "meta": {}, 
		    "parent_info": [], 
		    "flags": 0, 
		    "size": 10737418240, 
		    "order": 22, 
		    "name": "test2"
		}, 
		{
		    "parent_name": "", 
		    "parent_pool": 18446744073709551615, 
		    "features": 61, 
		    "lockers": [], 
		    "num_objs": 2560, 
		    "block_name_prefix": "rbd_data.5f4ba327b23c6", 
		    "old_format": False, 
		    "snaps": {}, 
		    "obj_size": 4194304, 
		    "meta": {}, 
		    "parent_info": [
			"images", 
			"test", 
			"snap"
		    ], 
		    "flags": 0, 
		    "size": 10737418240, 
		    "order": 22, 
		    "name": "clone1"
		}
	    ], 
	    "test2": []
	}
    else:
        raise NotImplementedError()

def fake_get(fsid, obj_type, obj_id):
    if obj_type == POOL:
        return {'pool_name': 'test'}
    else:
        raise NotImplementedError()

class TestRbd(TestCase):

    def setUp(self):
        self.request = mock.Mock()

        with mock.patch('calamari_rest.views.v2.RPCViewSet'):
            self.cmvs = RbdViewSet()
            self.cmvs.client = mock.MagicMock()
            self.cmvs.client.get.return_value = fake_get('abc', POOL, 2)
            self.cmvs.client.get_sync_object = mock.MagicMock(side_effect=fake_sync_object)
            self.cmvs.client.update.return_value = ["request_id"]
            self.cmvs.client.delete.return_value = ["request_id"]
            self.cmvs.client.create.return_value = ["request_id"]

    def test_list(self):
        self.request.method = 'GET'
        response = self.cmvs.list(self.request, 12345, 2)
        self.assertEqual(response.status_code, 200)
 
    def test_retrieve(self):
        self.request.method = 'GET'
        response = self.cmvs.retrieve(self.request, 12345, 2, 'test')
        self.assertEqual(response.status_code, 200)

    def test_update(self):
        self.request.method = 'PUT'
        self.request.DATA = {'name': 'test-rename', 'size': 1024000}
        response = self.cmvs.update(self.request, 12345, 2, 'test')
        self.assertEqual(response.status_code, 202)

    def test_delete(self):
        self.request.method = 'DELETE'
        response = self.cmvs.destroy(self.request, 12345, 2, 'test')
        self.assertEqual(response.status_code, 202)

    def test_create(self):
        self.request.method = 'POST'
        self.request.DATA = {'name': 'test3', 'size': 1024000}
        response = self.cmvs.create(self.request, 12345, 2)
        self.assertEqual(response.status_code, 202)
