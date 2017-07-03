import glob

def linux_disks():
    '''
    Return list of disk devices and work out if they are SSD or HDD.
    '''
    ret = {'disks': [], 'SSDs': []}

    for entry in glob.glob('/sys/block/*/queue/rotational'):
        with open(entry) as entry_fp:
            device = entry.split('/')[3]
            with open('/sys/block/' + device + '/size') as size_fp:
                size = size_fp.read()
            size = float(size) / (2 * 1024 * 1024)
            flag = entry_fp.read(1)
            if flag == '0':
                ret['SSDs'].append((device, size))
            elif flag == '1':
                ret['disks'].append((device, size))

    return ret
