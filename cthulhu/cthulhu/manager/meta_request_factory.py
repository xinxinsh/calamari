from cthulhu.manager.request_factory import RequestFactory
from cthulhu.manager.user_request import RbdRequest


class MetaRequestFactory(RequestFactory):
    def create(self, attributes):
	return RbdRequest(
            "Initiating Create Meta on {cluster_name}".format(cluster_name=self._cluster_monitor),
            self._cluster_monitor.fsid,
            self._cluster_monitor.name,
            [('set_metadata', attributes)])
    def delete(self, obj_id, attributes):
	return RbdRequest(
            "Initiating Delete Meta on {cluster_name}".format(cluster_name=self._cluster_monitor),
            self._cluster_monitor.fsid,
            self._cluster_monitor.name,
            [('remove_metadata', attributes)])
    def update(self, obj_id, attributes):
	return RbdRequest(
            "Initiating Update RBD on {cluster_name}".format(cluster_name=self._cluster_monitor),
            self._cluster_monitor.fsid,
            self._cluster_monitor.name,
            [('set_metadata', attributes)])
