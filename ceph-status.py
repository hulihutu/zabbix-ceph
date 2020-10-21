#!/usr/bin/env python
# -*- coding: UTF-8 -*-

import sys
import time
import commands
import json
import subprocess
import argparse


class CephState(object):
    def __init__(self):
        self.ceph_state_file = "/var/log/zabbix/ceph_status.json"
        self.ceph_df_file = "/var/log/zabbix/ceph_df.json"
        self.ceph_pool_state_file = "/var/log/zabbix/ceph_pool_state.json"
        self.ceph_rgw_bucket_state_file = "/var/log/zabbix/ceph_rgw_bucket_state.json"

    def loadData(self, filepath):
        '''Deserialize ceph status files  to a Python object.
        :param filepath:
        :return:
        '''
        for step in range(3):
            try:
                with open(filepath, 'r') as f:
                    jsonout = json.load(f)
                return jsonout
            except Exception:
                time.sleep(1)
        else:
            raise Exception("Failed to read file {}".format(filepath))

    def get_cluster_health(self):
        cluster_health = commands.getoutput('timeout 10 ceph health -f json-pretty 2>/dev/null')
        try:
            jsonout = json.loads(cluster_health)
            if jsonout["status"] == "HEALTH_OK":
                return 1
            elif jsonout["status"] == "HEALTH_WARN":
                return 2
            elif jsonout["status"] == "HEALTH_ERR":
                return 3
            else:
                return 255
        except:
            return 255

    def get_cluster_active_mon(self):
        jsonout = self.loadData(self.ceph_state_file)
        return len(jsonout["quorum_names"])

    def get_cluster_osd_state(self, arg):
        '''get cluster osd state num
        '''
        jsonout = self.loadData(self.ceph_state_file)
        if arg == 'total':
            return jsonout["osdmap"]['osdmap']['num_osds']
        elif arg == 'up':
            return jsonout["osdmap"]['osdmap']['num_up_osds']
        elif arg == 'in':
            return jsonout["osdmap"]['osdmap']['num_in_osds']
        elif arg == 'max_commit':
            return self.get_cluster_latency('max_commit')
        elif arg == 'max_apply':
            return self.get_cluster_latency('max_apply')
        else:
            return 0

    def get_cluster_used_percent(self):
        '''get cluster used percent
        '''
        jsonout = self.loadData(self.ceph_state_file)

        cluster_used = int(jsonout["pgmap"]["bytes_used"])
        cluster_total = int(jsonout["pgmap"]["bytes_total"])
        return "%.3f" % (cluster_used / float(cluster_total) * 100)

    def get_cluster_pgs_state(self, arg):
        '''get cluster pg state
        '''
        jsonout = self.loadData(self.ceph_state_file)

        if arg == 'total':
            return jsonout["pgmap"]["num_pgs"]
        elif arg == 'active':
            for pgs_state in jsonout["pgmap"]["pgs_by_state"]:
                if pgs_state["state_name"] == 'active+clean':
                    return pgs_state["count"]
            else:
                return 0
        elif arg == 'peering':
            for pgs_state in jsonout["pgmap"]["pgs_by_state"]:
                if "peering" in pgs_state["state_name"]:
                    return pgs_state["count"]
            else:
                return 0
        else:
            count_list = [0]
            for pgs_state in jsonout["pgmap"]["pgs_by_state"]:
                if arg in pgs_state["state_name"].split('+'):
                    count_list.append(int(pgs_state["count"]))
            return max(count_list)

    def get_cluster_latency(self, arg):
        '''get cluster max latency
        '''

        stats_to_fetch = {
            "max_commit": "commit_latency_ms",
            "max_apply": "apply_latency_ms"
        }

        #test test
        if arg in stats_to_fetch:
            get_cluster_latency = commands.getoutput('timeout 10 ceph osd perf -f json-pretty 2>/dev/null')
            jsonout = json.loads(get_cluster_latency)

            osd_perf_list = [int(item["perf_stats"][stats_to_fetch.get(arg)]) for item in jsonout["osd_perf_infos"]]

            return max(osd_perf_list)

    def get_cluster_throughput(self, arg):
        '''get cluster throughput write and read
        '''
        jsonout = self.loadData(self.ceph_state_file)

        val = jsonout["pgmap"].get(arg, 0)
        return val

    def get_cluster_total_ops(self, arg):
        '''get cluster throughput write and read
        '''

        stats_to_fetch = {
            "rps": "read_op_per_sec",
            "wps": "write_op_per_sec",
            "pps": "promote_op_per_sec"
        }

        jsonout = self.loadData(self.ceph_state_file)

        if arg == "ops":
            ops_list = [jsonout["pgmap"].get(value, 0) for value in stats_to_fetch.values()]
            return sum(ops_list)
        elif arg in stats_to_fetch:
            ops = jsonout["pgmap"].get(stats_to_fetch.get(arg), 0)
            return ops
        else:
            return 0

    def get_cluster_total_pools(self):
        jsonout = self.loadData(self.ceph_df_file)
        pool_lst = [item['name'] for item in jsonout["pools"]]

        return len(pool_lst)

    def get_cluster_pools(self):
        '''get all pool name
        '''
        cluster_pools = commands.getoutput('timeout 10 ceph osd lspools  -f json-pretty 2>/dev/null')
        jsonout = json.loads(cluster_pools)
        pool_list = [{"{#POOLNAME}": str(item['poolname'])} for item in jsonout]
        return json.dumps({'data': pool_list}, indent=4, separators=(',', ':'))

    def get_mds_subdirs(self):
        '''get all fs_sub_dir name
        '''
        try:
            # get fs_sub_dir rootdir:/mnt/cephfs  limit 200
            args = "timeout 10 ls -l /mnt/cephfs |grep '^d' | awk '{print $9}' | head -200"
            t = subprocess.Popen(args, shell=True, stdout=subprocess.PIPE).communicate()[0]
            subdirs = [{'{#FSDIR_NAME}': dir} for dir in t.split('\n') if len(dir) != 0]

            return json.dumps({'data': subdirs}, indent=4, separators=(',', ':'))
        except:
            return json.dumps({'data': []}, indent=4, separators=(',', ':'))

    def get_host_osds(self):
        '''get all osd
        '''
        args = "mount|grep osd|awk '{print $3}'|cut -f2 -d - 2>/dev/null"
        t = subprocess.Popen(args, shell=True, stdout=subprocess.PIPE).communicate()[0]
        osds = [{'{#OSD}': osd} for osd in t.split('\n') if len(osd) != 0]
        return json.dumps({'data': osds}, indent=4, separators=(',', ':'))

    def get_osd_mem_virt(self, osd, memtype):
        pidfile = "/var/run/ceph/osd.{0}.pid".format(osd)
        osdpid = commands.getoutput('cat {0}  2>/dev/null'.format(pidfile))
        if not osdpid:
            return 0
        elif memtype == "virt":
            osd_runmemvsz = commands.getoutput('ps -p {0}  -o vsz |grep -v VSZ 2>/dev/null'.format(osdpid))
            return osd_runmemvsz
        elif memtype == "res":
            osd_runmemrsz = commands.getoutput('ps -p {0}  -o rsz |grep -v RSZ 2>/dev/null'.format(osdpid))
            return osd_runmemrsz

    def get_osd_cpu(self, osd):
        pidfile = "/var/run/ceph/osd.{0}.pid".format(osd)
        osdpid = commands.getoutput('cat {0}  2>/dev/null'.format(pidfile))
        if not osdpid:
            return 0
        osd_cpu = commands.getoutput(
            '''ps -p {0}  -o pcpu |grep -v CPU|awk 'gsub(/^ *| *$/,"")' 2>/dev/null'''.format(osdpid))
        return osd_cpu

    def get_pool_df(self, poolname, arg):
        jsonout = self.loadData(self.ceph_df_file)

        # pool_lst = [ item['name'] for item in jsonout["pools"] ]
        # if not poolname in pool_lst:
        #     raise Exception("Error ENOENT: unrecognized pool {0}".format(poolname))

        for item in jsonout["pools"]:
            if item["name"] == poolname:
                return item["stats"][arg]
        else:
            raise Exception("Error ENOENT: unrecognized pool {0}".format(poolname))

    def get_pool_io_rate(self, poolname, stats):
        '''get every pool throughput,ops
        '''
        jsonout = self.loadData(self.ceph_pool_state_file)

        for item in jsonout:
            if item["pool_name"] == poolname:
                return item["client_io_rate"].get(stats, 0)

    def get_pool_config(self, poolname, config):
        '''get cluster pool config
        '''
        if config == "id":
            pool_id = commands.getoutput(
                "timeout 10 ceph  osd pool get {0} size -f json-pretty 2>/dev/null".format(poolname))
            jsonout = json.loads(pool_id)
            return jsonout["pool_id"]
        else:
            try:
                pool_cmd = commands.getoutput(
                    "timeout 10 ceph osd pool get {0} {1} -f json-pretty 2>/dev/null".format(poolname, config))
                jsonout = json.loads(pool_cmd)
                return jsonout[config]
            except ValueError:
                raise Exception('Error EINVAL: invalid command')

    def get_rgw_bucket_stats(self, config):
        '''get cluster rgw bucket stats
        '''
        jsonout = self.loadData(self.ceph_rgw_bucket_state_file)

        stats_to_fetch = {
            "max_shard": "objects_per_shard",
            "max_bucket": "num_objects"
        }

        if not config in stats_to_fetch:
            return "Invalid command: {} not!".format(config)

        count_list = []
        for check in jsonout:
            for bucket_data in check['buckets']:
                count_list.append(bucket_data[stats_to_fetch[config]])
        return max(count_list)

    def get_fsdir_config(self, fsdir_name, config):
        rootdir = "/mnt/cephfs"
        if config == "fsdir_max_bytes":
            args = 'timeout 10 /usr/bin/getfattr -n ceph.quota.max_bytes {0} |grep "ceph.quota.max_bytes"|grep -o "[0-9]*"'.format(
                fsdir_name)
            p = subprocess.Popen(args, shell=True, cwd=rootdir, stdout=subprocess.PIPE,
                                 stderr=subprocess.PIPE).communicate()
            if p[0]:
                value = p[0].strip('\n')
            elif 'No such attribute' in p[1]:
                value = 0
            else:
                value = None
            return value
        elif config == "fsdir_used":
            args = 'timeout 10 /usr/bin/getfattr -d -m ceph.dir.rbytes {0} |grep "ceph.dir.rbytes"| grep -o "[0-9]*"'.format(
                fsdir_name)
            p = subprocess.Popen(args, shell=True, cwd=rootdir, stdout=subprocess.PIPE,
                                 stderr=subprocess.PIPE).communicate()

            value = p[0].strip('\n') if p[0] else None
            return value


def main():
    parser = argparse.ArgumentParser(description='ceph state', usage='%(prog)s [options]')
    parser.add_argument('-v', '--version', action='version', version='%(prog)s 1.0')

    parser.add_argument('-k', '--keys', nargs='+', dest='keys', metavar=('{ key1 }'), help='key')
    parser.add_argument('-p', '--poolname', nargs=1, dest='poolname', metavar=('{ pool_name }'), help='poolname')

    if len(sys.argv) == 1:
        print(parser.print_help())
    else:
        args = parser.parse_args()
        cephstate = CephState()
        if args.poolname:
            poolname = args.poolname[0]
            if poolname in ["lists", "list"]:
                print(cephstate.get_cluster_pools())
                sys.exit(0)
            if args.keys:
                if len(args.keys) == 2:
                    if args.keys[0] == 'config':
                        print(cephstate.get_pool_config(poolname, args.keys[1]))
                    elif args.keys[0] == 'io':
                        print(cephstate.get_pool_io_rate(poolname, args.keys[1]))
                    elif args.keys[0] == 'df':
                        print(cephstate.get_pool_df(poolname, args.keys[1]))
                else:
                    pass
            sys.exit(0)

        if args.keys:
            if len(args.keys) == 2:
                if args.keys[0] == 'osd':
                    print(cephstate.get_cluster_osd_state(args.keys[1]))
                elif args.keys[0] == 'pg':
                    print(cephstate.get_cluster_pgs_state(args.keys[1]))
                elif args.keys[0] == 'rados':
                    print(cephstate.get_cluster_throughput(args.keys[1]))
                elif args.keys[0] == 'rgw':
                    print(cephstate.get_rgw_bucket_stats(args.keys[1]))
                else:
                    print(parser.print_help())
            else:
                ops_list = ['ops', 'rps', 'wps']
                if args.keys[0] == 'mon':
                    print(cephstate.get_cluster_active_mon())
                elif args.keys[0] == 'health':
                    print(cephstate.get_cluster_health())
                elif args.keys[0] in ops_list:
                    print(cephstate.get_cluster_total_ops(args.keys[0]))
                else:
                    print(parser.print_help())
        else:
            print(parser.print_help())


if __name__ == '__main__':
    main()
