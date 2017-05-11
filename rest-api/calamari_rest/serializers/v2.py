
from rest_framework import serializers
from calamari_common.db.event import severity_str, SEVERITIES
import calamari_rest.serializers.fields as fields
from calamari_common.types import CRUSH_RULE_TYPE_REPLICATED, CRUSH_RULE_TYPE_ERASURE, USER_REQUEST_COMPLETE, \
    USER_REQUEST_SUBMITTED, OSD_FLAGS


class ValidatingSerializer(serializers.Serializer):

    def is_valid(self, http_method):

        self._errors = super(ValidatingSerializer, self).errors or {}

        if self.init_data is not None:
            if http_method == 'POST':
                self._errors.update(self.construct_errors(self.Meta.create_allowed,
                                                          self.Meta.create_required,
                                                          self.init_data.keys(),
                                                          http_method))

            elif http_method in ('PATCH', 'PUT'):
                self._errors.update(self.construct_errors(self.Meta.modify_allowed,
                                                          self.Meta.modify_required,
                                                          self.init_data.keys(),
                                                          http_method))
            else:
                self._errors.update([[http_method, 'Not a valid method']])

        return not self._errors

    def construct_errors(self, allowed, required, init_data, action):
        errors = {}

        not_allowed = set(init_data) - set(allowed)
        errors.update(dict([x, 'Not allowed during %s' % action] for x in not_allowed))

        required = set(required) - set(init_data)
        errors.update(dict([x, 'Required during %s' % action] for x in required))

        return errors

    def get_data(self):
        # like http://www.django-rest-framework.org/api-guide/serializers#dynamically-modifying-fields
        filtered_data = {}
        for field, value in self.init_data.iteritems():
            # TODO HACK this assumes that we are dealing with a nested serializer field
            # see http://tracker.ceph.com/issues/10557
            # and http://tracker.ceph.com/issues/10556
            if isinstance(self.fields[field], serializers.Serializer) and self.fields[field].many:
                filtered_data[field] = []
                for datum in self.data[field]:
                    datum = dict(datum) if isinstance(datum, dict) else datum
                    filtered_data[field].append(datum)
            else:
                filtered_data[field] = self.data[field]

        return filtered_data


class ClusterSerializer(serializers.Serializer):
    class Meta:
        fields = ('update_time', 'id', 'name')

    update_time = serializers.DateTimeField(
        help_text="The time at which the last status update from this cluster was received"
    )
    name = serializers.Field(
        help_text="Human readable cluster name, not a unique identifier"
    )
    id = serializers.Field(
        help_text="The FSID of the cluster, universally unique"
    )


class PoolSerializer(ValidatingSerializer):
    class Meta:
        fields = ('name', 'id', 'size', 'pg_num', 'crush_ruleset', 'min_size', 'crash_replay_interval', 'crush_ruleset',
                  'pgp_num', 'hashpspool', 'full', 'quota_max_objects', 'quota_max_bytes')
        create_allowed = ('name', 'pg_num', 'pgp_num', 'size', 'min_size', 'crash_replay_interval', 'crush_ruleset',
                          'quota_max_objects', 'quota_max_bytes', 'hashpspool')
        create_required = ('name', 'pg_num')
        modify_allowed = ('name', 'pg_num', 'pgp_num', 'size', 'min_size', 'crash_replay_interval', 'crush_ruleset',
                          'quota_max_objects', 'quota_max_bytes', 'hashpspool')
        modify_required = ()

    # Required in creation
    name = serializers.CharField(required=False, source='pool_name',
                                 help_text="Human readable name of the pool, may"
                                 "change over the pools lifetime at user request.")
    pg_num = serializers.IntegerField(required=False,
                                      help_text="Number of placement groups in this pool")

    # Not required in creation, immutable
    id = serializers.CharField(source='pool', required=False, help_text="Unique numeric ID")

    # May be set in creation or updates
    size = serializers.IntegerField(required=False,
                                    help_text="Replication factor")
    min_size = serializers.IntegerField(required=False,
                                        help_text="Minimum number of replicas required for I/O; clamped to 'size' if greater; 0 defaults to 'size - int(size/2)'")
    crash_replay_interval = serializers.IntegerField(required=False,
                                                     help_text="Number of seconds to allow clients to "
                                                               "replay acknowledged, but uncommitted requests")
    crush_ruleset = serializers.IntegerField(required=False, help_text="CRUSH ruleset in use")
    # In 'ceph osd pool set' it's called pgp_num, but in 'ceph osd dump' it's called
    # pg_placement_num :-/
    pgp_num = serializers.IntegerField(source='pg_placement_num', required=False,
                                       help_text="Effective number of placement groups to use when calculating "
                                                 "data placement")

    # This is settable by 'ceph osd pool set' but in 'ceph osd dump' it only appears
    # within the 'flags' integer.  We synthesize a boolean from the flags.
    hashpspool = serializers.BooleanField(required=False, help_text="Enable HASHPSPOOL flag")

    # This is synthesized from ceph's 'flags' attribute, read only.
    full = serializers.BooleanField(required=False, help_text="True if the pool is full")

    quota_max_objects = serializers.IntegerField(required=False,
                                                 help_text="Quota limit on object count (0 is unlimited)")
    quota_max_bytes = serializers.IntegerField(required=False,
                                               help_text="Quota limit on usage in bytes (0 is unlimited)")


class OsdSerializer(ValidatingSerializer):
    class Meta:
        fields = ('uuid', 'up', 'in', 'id', 'reweight', 'server', 'pools', 'valid_commands', 'public_addr', 'cluster_addr', 'crush_node_ancestry', 'backend_partition_path', 'backend_device_node')
        create_allowed = ()
        create_required = ()
        modify_allowed = ('up', 'in', 'reweight')
        modify_required = ()

    id = serializers.IntegerField(read_only=True, source='osd', help_text="ID of this OSD within this cluster")
    uuid = fields.UuidField(read_only=True, help_text="Globally unique ID for this OSD")
    up = fields.BooleanField(required=False, help_text="Whether the OSD is running from the point of view of the rest of the cluster")
    _in = fields.BooleanField(required=False, help_text="Whether the OSD is 'in' the set of OSDs which will be used to store data")
    reweight = serializers.FloatField(required=False, help_text="CRUSH weight factor")
    server = serializers.CharField(read_only=True, help_text="FQDN of server this OSD was last running on")
    pools = serializers.Field(help_text="List of pool IDs which use this OSD for storage")
    valid_commands = serializers.CharField(read_only=True, help_text="List of commands that can be applied to this OSD")

    public_addr = serializers.CharField(read_only=True, help_text="Public/frontend IP address")
    cluster_addr = serializers.CharField(read_only=True, help_text="Cluster/backend IP address")
    crush_node_ancestry = serializers.Field(help_text="An ordered list of CRUSH node ids that represent a path from the parent node of this OSD up to the root of the tree")
    backend_partition_path = serializers.CharField(read_only=True, help_text="Full path to the storage partition targeted by this OSD")
    backend_device_node = serializers.CharField(read_only=True, help_text="Physical device node that the OSD's targeted partition is provisioned from")

# Declarative metaclass definitions are great until you want
# to use a reserved word
OsdSerializer.base_fields['in'] = OsdSerializer.base_fields['_in']

class OsddfSerializer(serializers.Serializer):
    class Meta:
        fields = ('name', 'utilization', 'kb', 'kb_avail', 'kb_used', 'var', 'crush_weight', 'reweight', 'pgs')
 
    name = serializers.CharField(help_text='OSD Name')
    utilization = serializers.FloatField(help_text='OSD Utilization')
    kb = serializers.IntegerField(help_text='OSD Total Size')
    kb_avail = serializers.IntegerField(help_text='OSD Available Size')
    kb_used = serializers.IntegerField(help_text='OSD Used Size')
    var = serializers.FloatField(help_text='OSD Variation')
    crush_weight = serializers.FloatField(help_text='OSD Crush Weight')
    reweight = serializers.FloatField(help_text='OSD Reweight')
    pgs = serializers.IntegerField(help_text='OSD PG Count')

class OsdperfSerializer(serializers.Serializer):
    class Meta:
        fields = ('id', 'perf_stats')

    id = serializers.IntegerField(help_text='OSD ID')
    perf_stats = serializers.CharField(help_text='OSD Perf Stat')


class OsdConfigSerializer(ValidatingSerializer):
    class Meta:
        fields = OSD_FLAGS
        create_allowed = ()
        create_required = ()
        modify_allowed = OSD_FLAGS
        modify_required = ()

    pause = serializers.BooleanField(help_text="Disable IO requests to all OSDs in cluster", required=False)
    noup = serializers.BooleanField(help_text="Prevent OSDs from automatically getting marked as Up by the monitors. This setting is useful for troubleshooting", required=False)
    nodown = serializers.BooleanField(help_text="Prevent OSDs from automatically getting marked as Down by the monitors. This setting is useful for troubleshooting", required=False)
    noout = serializers.BooleanField(help_text="Prevent Down OSDs from being marked as out", required=False)
    noin = serializers.BooleanField(help_text="Prevent OSDs from booting OSDs from being marked as IN. Will cause cluster health to be set to WARNING", required=False)
    nobackfill = serializers.BooleanField(help_text="Disable backfill operations on cluster", required=False)
    norecover = serializers.BooleanField(help_text="Disable replication of Placement Groups", required=False)
    noscrub = serializers.BooleanField(help_text="Disables automatic periodic scrub operations on OSDs. May still be initiated on demand", required=False)
    nodeepscrub = serializers.BooleanField(help_text="Disables automatic periodic deep scrub operations on OSDs. May still be initiated on demand", required=False)


OsdConfigSerializer.base_fields['nodeep-scrub'] = OsdConfigSerializer.base_fields['nodeepscrub']

class StepItemSerializer(serializers.Serializer):
    op = serializers.CharField(source='op', help_text="Human readable name", required=True)
    type = serializers.CharField(required=False)
    num = serializers.IntegerField(required=False)
    item = serializers.IntegerField(required=False)
    item_name = serializers.CharField(help_text="Human readable name", required=False)

    class Meta:
        fields = ('op', 'type', 'num', 'item_name', 'item')

def less_than(limit):
    def compare(value):
        if value > limit:
            raise serializers.ValidationError('This field must be less than %s.' % str(limit))
    return compare

class CrushRuleSerializer(ValidatingSerializer):
    class Meta:
        fields = ('id', 'name', 'ruleset', 'type', 'min_size', 'max_size', 'steps', 'osd_count')
        create_allowed = ('name', 'ruleset', 'type', 'min_size', 'max_size', 'steps')
        create_required = ('name', 'type', 'min_size', 'max_size', 'steps')
        modify_allowed = ('name', 'ruleset', 'min_size', 'max_size', 'steps')
        modify_required = ()

    id = serializers.IntegerField(source='rule_id', required=False)
    name = serializers.CharField(source='rule_name', help_text="Human readable name")
    ruleset = serializers.IntegerField(help_text="ID of the CRUSH ruleset of which this rule is a member", required=False, validators=[less_than(255), ])
    type = fields.EnumField({CRUSH_RULE_TYPE_REPLICATED: 'replicated', CRUSH_RULE_TYPE_ERASURE: 'erasure'}, help_text="Data redundancy type", required=False)
    min_size = serializers.IntegerField(
        help_text="If a pool makes more replicas than this number, CRUSH will NOT select this rule", required=False)
    max_size = serializers.IntegerField(
        help_text="If a pool makes fewer replicas than this number, CRUSH will NOT select this rule", required=False)
#    steps = StepItemSerializer(required=True, many=True, help_text="A bucket may have one or more items. The items may consist of node buckets or leaves. Items may have a weight that reflects the relative weight of the item.")
    steps = serializers.Field(help_text="List of operations used to select OSDs")
    osd_count = serializers.IntegerField(help_text="Number of OSDs which are used for data placement", required=False)


class CrushTypeSerializer(serializers.Serializer):
    class Meta:
        fields = ('id', 'name')

    name = serializers.CharField(
        help_text="Human readable type name, not a unique identifier"
    )
    id = serializers.IntegerField(
        help_text="The id used to identify the type within the CRUSH map, unique",
        source="type_id"
    )


class NodeItemSerializer(serializers.Serializer):
    id = serializers.IntegerField()
    weight = serializers.FloatField()
    pos = serializers.IntegerField()

    class Meta:
        fields = ('id', 'weight', 'pos')


class CrushNodeSerializer(ValidatingSerializer):
    class Meta:
        fields = ('bucket_type', 'name', 'id', 'weight', 'alg', 'hash', 'items')
        create_allowed = ('bucket_type', 'name', 'items', 'id')
        create_required = ()
        modify_allowed = ('bucket_type', 'name', 'items')
        modify_required = ()

    # TODO need to validate that this is a valid type ? use choice field?
    bucket_type = serializers.CharField(help_text="Buckets facilitate a hierarchy of nodes and leaves. Node (or non-leaf) buckets typically represent physical locations in a hierarchy. e.g. host, rack, datacenter",
                                        source="type_name",
                                        )
    name = serializers.CharField(label='bucket-name', help_text="unique name")
    id = serializers.IntegerField(required=False, help_text="unique ID expressed as an integer (optional)")
    weight = serializers.FloatField(required=False, help_text="the relative capacity/capability of the item(s)")
    alg = serializers.ChoiceField(required=False,
                                  help_text="bucket algorithm",
                                  choices=list(enumerate(('straw', 'uniform', 'list', 'tree'))),
                                  default=0)
    _hash = serializers.IntegerField(required=False, help_text="hash algorithm", default=0)
    items = NodeItemSerializer(required=False, many=True, help_text="A bucket may have one or more items. The items may consist of node buckets or leaves. Items may have a weight that reflects the relative weight of the item.")


CrushNodeSerializer.base_fields['hash'] = CrushNodeSerializer.base_fields['_hash']
CrushNodeSerializer.base_fields['bucket_type'] = CrushNodeSerializer.base_fields['bucket_type']


class CrushRuleSetSerializer(serializers.Serializer):
    class Meta:
        fields = ('id', 'rules')

    id = serializers.IntegerField()
    rules = CrushRuleSerializer(many=True)


class RequestSerializer(serializers.Serializer):
    class Meta:
        fields = ('id', 'state', 'error', 'error_message', 'headline', 'status', 'requested_at', 'completed_at')

    id = serializers.CharField(help_text="A globally unique ID for this request")
    state = serializers.CharField(help_text="One of '{complete}', '{submitted}'".format(
        complete=USER_REQUEST_COMPLETE, submitted=USER_REQUEST_SUBMITTED))
    error = serializers.BooleanField(help_text="True if the request completed unsuccessfully")
    error_message = serializers.CharField(help_text="Human readable string describing failure if ``error`` is True")
    headline = serializers.CharField(help_text="Single sentence human readable description of the request")
    status = serializers.CharField(help_text="Single sentence human readable description of the request's current "
                                             "activity, if it has more than one stage.  May be null.")
    requested_at = serializers.DateTimeField(help_text="Time at which the request was received by calamari server")
    completed_at = serializers.DateTimeField(help_text="Time at which the request completed, may be null.")


class SaltKeySerializer(ValidatingSerializer):
    class Meta:
        fields = ('id', 'status')
        create_allowed = ()
        create_required = ()
        modify_allowed = ('status',)
        modify_required = ()

    id = serializers.CharField(required=False, help_text="The minion ID, usually equal to a host's FQDN")
    status = serializers.CharField(help_text="One of 'accepted', 'rejected' or 'pre'")


class ServiceSerializer(serializers.Serializer):
    class Meta:
        fields = ('fsid', 'type', 'id', 'running')

    fsid = serializers.SerializerMethodField("get_fsid")
    type = serializers.SerializerMethodField("get_type")
    id = serializers.SerializerMethodField("get_id")
    running = serializers.BooleanField()

    def get_fsid(self, obj):
        return obj['id'][0]

    def get_type(self, obj):
        return obj['id'][1]

    def get_id(self, obj):
        return obj['id'][2]


class SimpleServerSerializer(serializers.Serializer):
    class Meta:
        fields = ('fqdn', 'hostname', 'managed', 'last_contact', 'boot_time', 'ceph_version', 'services')

    # Identifying information
    fqdn = serializers.CharField(help_text="Fully qualified domain name")
    hostname = serializers.CharField(help_text="Unqualified hostname")

    # Calamari monitoring status
    managed = serializers.BooleanField(
        help_text="True if this server is under Calamari server's control, false"
                  "if the server's existence was inferred via Ceph cluster maps.")
    last_contact = serializers.DateTimeField(
        help_text="The time at which this server last communicated with the Calamari"
                  "server.  This is always null for unmanaged servers")
    boot_time = serializers.DateTimeField(
        help_text="The time at which this server booted. "
                  "This is always null for unmanaged servers")
    ceph_version = serializers.CharField(
        help_text="The version of Ceph installed.  This is always null for unmanaged servers."
    )
    # Ceph usage
    services = ServiceSerializer(many=True, help_text="List of Ceph services seen"
                                 "on this server")


class ServerSerializer(SimpleServerSerializer):
    class Meta:
        fields = ('fqdn', 'hostname', 'services', 'frontend_addr', 'backend_addr',
                  'frontend_iface', 'backend_iface', 'managed',
                  'last_contact', 'boot_time', 'ceph_version')

    # Ceph network configuration
    frontend_addr = serializers.CharField()  # may be null if no OSDs or mons on server
    backend_addr = serializers.CharField()  # may be null if no OSDs on server
    frontend_iface = serializers.CharField()  # may be null if interface for frontend addr not up
    backend_iface = serializers.CharField()  # may be null if interface for backend addr not up


class EventSerializer(serializers.Serializer):
    class Meta:
        fields = ('when', 'severity', 'message')

    when = serializers.DateTimeField(help_text="Time at which event was generated")
    severity = serializers.SerializerMethodField('get_severity')
    message = serializers.CharField(help_text="One line human readable description")

    def get_severity(self, obj):
        return severity_str(obj.severity)

# django_rest_framework 2.3.12 doesn't let me put help_text on a methodfield
# https://github.com/tomchristie/django-rest-framework/pull/1594
EventSerializer.base_fields['severity'].help_text = "One of %s" % ",".join(SEVERITIES.values())


class LogTailSerializer(serializers.Serializer):
    """
    Trivial serializer to wrap a string blob of log output
    """
    class Meta:
        fields = ('lines',)

    lines = serializers.CharField("Retrieved log data as a newline-separated string")


class ConfigSettingSerializer(serializers.Serializer):
    class Meta:
        fields = ('key', 'value')

    # This is very simple for now, but later we may add more things like
    # schema information, allowed values, defaults.

    key = serializers.CharField(help_text="Name of the configuration setting")
    value = serializers.CharField(help_text="Current value of the setting, as a string")


class MonSerializer(serializers.Serializer):
    class Meta:
        fields = ('name', 'rank', 'in_quorum', 'server', 'addr')

    name = serializers.CharField(help_text="Human readable name")
    rank = serializers.IntegerField(help_text="Unique of the mon within the cluster")
    in_quorum = serializers.BooleanField(help_text="True if the mon is a member of current quorum")
    server = serializers.CharField(help_text="FQDN of server running the OSD")
    addr = serializers.CharField(help_text="IP address of monitor service")


class CliSerializer(serializers.Serializer):
    class Meta:
        fields = ('out', 'err', 'status')

    out = serializers.CharField(help_text="Standard out")
    err = serializers.CharField(help_text="Standard error")
    status = serializers.IntegerField(help_text="Exit code")

class PgSerializer(serializers.Serializer):
    class Meta:
        fields = ('name', 'state', 'up_primary', 'acting_primary', 'up', 'acting')

    name = serializers.CharField(source='pgid', help_text="PG name")
    state = serializers.CharField(help_text="PG state")
    up_primary = serializers.IntegerField(help_text="Up Primary")
    acting_primary = serializers.IntegerField(help_text="Acting Primary")
    up = serializers.IntegerField(help_text="Up Set")
    acting = serializers.IntegerField(help_text="Acting Set")

class RbdSerializer(ValidatingSerializer):
    class Meta:
        fields = ('name', 'prefix', 'size', 'obj_size', 'order', 'old_format', 'flags', 'features', 'num_objs', 'parent_name', 'parent_pool')
        create_allowed = ('name', 'size', 'order', 'features', 'old_format', 'stripe_unit', 'stripe_count')
        create_required = ('name', 'size')
        modify_allowed = ('name', 'size', 'features')
        modify_required = ()

    name =  serializers.CharField(help_text="RBD Name")
    prefix =  serializers.CharField(source='block_name_prefix', help_text="Block Name Prefix", required=False)
    size = serializers.IntegerField(help_text="RBD Size")
    obj_size = serializers.IntegerField(help_text="RBD Object Size", required=False)
    order = serializers.IntegerField(help_text="RBD Object Order", required=False)
    old_format = serializers.BooleanField(help_text="Old Format", required=False)
    flags = serializers.IntegerField(help_text="RBD Flags", required=False)
    features = serializers.IntegerField(help_text="RBD Features", required=False)
    num_objs = serializers.IntegerField(help_text="Number of Objects", required=False)
    parent_name = serializers.CharField(help_text="RBD Parent Name", required=False)
    parent_pool = serializers.CharField(help_text="RBD Parent Pool", required=False)

class CloneSerializer(serializers.Serializer):
    class Meta:
        fields = ('pool', 'name')

    pool =  serializers.CharField(help_text='Pool Name')
    name =  serializers.CharField(help_text='RBD Name')

class SnapSerializer(ValidatingSerializer):
    class Meta:
        fields = ('name', 'id', 'size', 'protected', 'children')
        create_allowed = ('name',)
        create_required = ('name',)
        modify_allowed = ('name', 'protected')
        modify_required = ()

    name =  serializers.CharField(help_text="Snap Name", required=False)
    id = serializers.IntegerField(help_text="Snap ID", required=False)
    size = serializers.CharField(help_text="Snap Size", required=False)
    protected = serializers.BooleanField(help_text="Snap Protected Flag", required=False)
    children = CloneSerializer(many=True, help_text="List of children", required=False)

class LockSerializer(serializers.Serializer):
    class Meta:
        fields = ('lockers', 'exclusive', 'tag')

    lockers =  serializers.CharField(help_text="Locker Name")
    tag = serializers.CharField(help_text="Locker Tag")
    exclusive = serializers.BooleanField(help_text="Locker Exclusive Flag")

class MetaSerializer(serializers.Serializer):
    class Meta:
        fields = ('key', 'value')


    key = serializers.CharField(help_text="Name of Metadata")
    value = serializers.CharField(help_text="Value of Metadata")

