from cthulhu.manager.request_factory import RequestFactory
from cthulhu.manager.user_request import RadosRequest
from calamari_common.types import PG_IMPLEMENTED_COMMANDS


class PgRequestFactory(RequestFactory):
    def scrub(self, pgid):
        return RadosRequest(
        "Initiating scrub on {cluster_name} pg {pgid}".format(cluster_name=self._cluster_monitor.name, pgid=pgid),
        self._cluster_monitor.fsid,
        self._cluster_monitor.name,
        [('pg scrub', {'pgid': str(pgid)})])

    def deepscrub(self, pgid):
        return RadosRequest(
        "Initiating deep-scrub on {cluster_name} pg {pgid}".format(cluster_name=self._cluster_monitor.name, pgid=pgid),
        self._cluster_monitor.fsid,
        self._cluster_monitor.name,
        [('pg deep-scrub', {'pgid': str(pgid)})])

    def repair(self, pgid):
        return RadosRequest(
        "Initiating repair on {cluster_name} pg {pgid}".format(cluster_name=self._cluster_monitor.name, pgid=pgid),
        self._cluster_monitor.fsid,
        self._cluster_monitor.name,
        [('pg repair', {'pgid': str(pgid)})])

    def get_valid_commands(self, pgids):
        return {'valid_commands': PG_IMPLEMENTED_COMMANDS}
