from cthulhu.manager.request_factory import RequestFactory
from cthulhu.manager.user_request import RadosRequest


class ConfigRequestFactory(RequestFactory):
    def update(self, key, attributes):
        attributes['args'] = {'injected_args' : ['--' + attributes['key'], attributes['value']]}
        del attributes['key']
        del attributes['value']
        commands = [('injectargs', {'data': attributes})]
        message = "update configuration {key} in cluster {cluster_name} to targets {targets}".format(cluster_name=self._cluster_monitor.name, key=key, targets=attributes['targets'])
        return RadosRequest(message, self._cluster_monitor.fsid, self._cluster_monitor.name, commands)
