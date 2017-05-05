from cthulhu.manager.request_factory import RequestFactory
from cthulhu.manager.user_request import RbdRequest


class SnapRequestFactory(RequestFactory):
    def create(self, attributes):
	return RbdRequest(
            "Initiating Create Snap on {cluster_name}".format(cluster_name=self._cluster_monitor),
            self._cluster_monitor.fsid,
            self._cluster_monitor.name,
            [('create_snapshot', attributes)])
    def delete(self, obj_id, attributes):
        attributes.update({'name': obj_id})
	return RbdRequest(
            "Initiating Delete Snap on {cluster_name}".format(cluster_name=self._cluster_monitor),
            self._cluster_monitor.fsid,
            self._cluster_monitor.name,
            [('remove_snapshot', attributes)])
    def update(self, obj_id, attributes):
        attributes.update({'name': obj_id})
        commands = []

        #  protect/unprotect snap
        if 'protected' in attributes:
            if attributes['protected']:
                commands.append(('protect_snapshot', attributes))
            else:
                commands.append(('unprotect_snapshot', attributes))

        # rename image
        # rename should be last operation of a set of update
        # image name is changed after rename
        if 'new_snap_name' in attributes and 'snap_name' in attributes:
            commands.append(('rename_snapshot', attributes))
      
	return RbdRequest(
            "Initiating Update Snap on {cluster_name}".format(cluster_name=self._cluster_monitor),
            self._cluster_monitor.fsid,
            self._cluster_monitor.name,
            commands)
