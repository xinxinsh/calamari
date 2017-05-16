#!/usr/bin/env python
import os
import sys
import site

prev_sys_path = list(sys.path)
sitedir = '/opt/calamari/venv/lib/python{maj}.{min}/site-packages'.format(
        maj=sys.version_info[0], min=sys.version_info[1])
site.addsitedir(sitedir)

# Reorder sys.path so new directories at the front.
new_sys_path = []
for item in list(sys.path):
    if item not in prev_sys_path:
        new_sys_path.append(item)
        sys.path.remove(item)
sys.path[:0] = new_sys_path

path = '/opt/calamari/webapp/calamari'
sys.path.append(path)
sys.path.append(path + '/calamari')

pwd = os.getcwd()

sys.path.append(pwd + '//minion-sim')


if __name__ == "__main__":
    # Load gevent so that runserver will behave itself when zeroRPC is used
    from gevent import monkey
    monkey.patch_all()

    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "calamari_web.settings")

    from django.core.management import execute_from_command_line

    execute_from_command_line(sys.argv)
