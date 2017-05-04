from cthulhu.manager.request_factory import RequestFactory
from cthulhu.manager.user_request import RbdRequest


class RbdRequestFactory(RequestFactory):
    def create(self, attributes):
	return RbdRequest(
            "Initiating Create RBD on {cluster_name}".format(cluster_name=self._cluster_monitor),
            self._cluster_monitor.fsid,
            self._cluster_monitor.name,
            [('create_image', attributes)])
    def delete(self, obj_id, attributes):
        attributes.update({'name': obj_id})
	return RbdRequest(
            "Initiating Delete RBD on {cluster_name}".format(cluster_name=self._cluster_monitor),
            self._cluster_monitor.fsid,
            self._cluster_monitor.name,
            [('remove_image', attributes)])
    def update(self, obj_id, attributes):

        commands = []
        # resize image
        if 'size' in attributes:
            commands.append(('resize_image', attributes))

        # update features
        if 'features' in attributes:
            commands.append(('update_features', attributes))

        # rename image
        # rename should be last operation of a set of update
        # image name is changed after rename
        if 'new_name' in attributes and 'name' in attributes:
            commands.append(('rename_image', attributes))
      
	return RbdRequest(
            "Initiating Update RBD on {cluster_name}".format(cluster_name=self._cluster_monitor),
            self._cluster_monitor.fsid,
            self._cluster_monitor.name,
            commands)
