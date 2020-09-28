#!/usr/bin/env python
# -*- coding: UTF-8 -*-

import os
import sys
import time
import commands
import json
import subprocess
import argparse


class CephState(object):
    def __init__(self):
        self.cephstate_cmd = 'timeout 10 ceph -s -f json-pretty 2>/dev/null'
   
        #self.ceph_state_file = "/var/log/zabbix/ceph_status.json"        
        self.ceph_state_file = "/var/log/zabbix/ceph_status.json"
        self.ceph_df_file = "/var/log/zabbix/ceph_df.json"        
        self.ceph_pool_state_file = "/var/log/zabbix/ceph_pool_state.json"      
        self.ceph_rgw_bucket_state_file = "/var/log/zabbix/ceph_rgw_bucket_state.json" 

    def loadData(self,filepath):
        '''Deserialize ceph status files  to a Python object.
        :param filepath:
        :return:
        '''
        for _ in range(3):
            try:
                with open(filepath,'r') as f:
                    json_str = json.load(f)
                return json_str
            except Exception as e:
                time.sleep(1)
        else:
            raise Exception(e)

    def get_cluster_health(self):
        cluster_health = commands.getoutput('timeout 10 ceph health -f json-pretty 2>/dev/null')
        try:
            json_str = json.loads(cluster_health)
            if json_str["status"] == "HEALTH_OK":
                return 1
            elif  json_str["status"] == "HEALTH_WARN":
                return 2
            elif  json_str["status"] == "HEALTH_ERR":
                return 3
            else:
                return 255
        except:
            return 255

    def get_cluster_active_mon(self):
        json_str = self.loadData(self.ceph_state_file)
        return len(json_str["quorum_names"])

    def get_cluster_osd_state(self,arg):
        '''get cluster osd state num
        '''
        json_str = self.loadData(self.ceph_state_file)
        if arg == 'total': 
            return json_str["osdmap"]['osdmap']['num_osds']
        elif arg == 'up':
            return json_str["osdmap"]['osdmap']['num_up_osds']
        elif arg == 'in':
            return json_str["osdmap"]['osdmap']['num_in_osds']
        elif arg == 'ave_commit':
            return self.get_cluster_latency('ave_commit')
        elif arg == 'ave_apply':
            return self.get_cluster_latency('ave_apply')
        else:
            return 0

    def get_cluster_used_percent(self):
        '''get cluster used percent
        '''
        json_str = self.loadData(self.ceph_state_file)

        cluster_used = int(json_str["pgmap"]["bytes_used"])
        cluster_total = int(json_str["pgmap"]["bytes_total"])
        return  "%.3f"   %(cluster_used/float(cluster_total)*100)

    def get_cluster_pgs_state(self,arg):
        '''get cluster pg state
        '''
        json_str = self.loadData(self.ceph_state_file)

        if arg == 'total':
            return json_str["pgmap"]["num_pgs"]
        elif arg == 'active':
            for pgs_state in json_str["pgmap"]["pgs_by_state"]:
               if pgs_state["state_name"] == 'active+clean':
                   return pgs_state["count"]
            else:
                return 0
        elif arg == 'peering':
            for pgs_state in json_str["pgmap"]["pgs_by_state"]:
               if "peering" in pgs_state["state_name"]:
                   return pgs_state["count"]
            else:
                return 0
        #取包含了相应状态的PG数目。注意：取值方式未必严谨，此监控值仅供参考。
        else:
            count_list = [0]
            for pgs_state in json_str["pgmap"]["pgs_by_state"]:
               if arg in pgs_state["state_name"].split('+'):
                   count_list.append(int(pgs_state["count"]))
            return max(count_list)

    def get_cluster_latency(self,arg):
        '''get cluster average latency
        '''
        if arg =="ave_commit":
            osd_commit_list = []
            get_cluster_latency_commit = commands.getoutput('timeout 10 ceph osd perf -f json-pretty 2>/dev/null')
            json_str = json.loads(get_cluster_latency_commit)

            for item in json_str["osd_perf_infos"]:
                osd_commit_list.append(int(item["perf_stats"]["commit_latency_ms"]))
            return sum(osd_commit_list)/len(osd_commit_list)

        if arg =="ave_apply":
            osd_apply_list = []
            get_cluster_latency_apply = commands.getoutput('timeout 10 ceph osd perf -f json-pretty 2>/dev/null')
            json_str = json.loads(get_cluster_latency_apply)

            for item in json_str["osd_perf_infos"]:
                osd_apply_list.append(int(item["perf_stats"]["apply_latency_ms"]))
            return sum(osd_apply_list)/len(osd_apply_list)

    def get_cluster_throughput(self,arg):
        '''get cluster throughput write and read
        '''
        json_str = self.loadData(self.ceph_state_file)

        if arg == "write":
            if json_str["pgmap"].has_key("write_bytes_sec") == True:
                return  json_str["pgmap"]["write_bytes_sec"]
            else:
                return 0
        elif arg == "read":
            if json_str["pgmap"].has_key("read_bytes_sec") == True:
                return json_str["pgmap"]["read_bytes_sec"]
            else:
                return 0
        else:
            if json_str["pgmap"].has_key(arg) == True:
                return  json_str["pgmap"][arg]
            else:
                return 0

    def get_cluster_total_ops(self,arg):
        '''get cluster throughput write and read
        '''
        ops_list =[]
      
        json_str = self.loadData(self.ceph_state_file)

        if json_str["pgmap"].has_key('write_op_per_sec') == True:
            wps = json_str["pgmap"]["write_op_per_sec"]
            ops_list.append(int(wps))
        else:
            wps = 0
        if json_str["pgmap"].has_key('read_op_per_sec') == True:
            rps = json_str["pgmap"]["read_op_per_sec"]
            ops_list.append(int(rps))
        else:
            rps = 0
        if json_str["pgmap"].has_key('promote_op_per_sec') == True:
            pps = json_str["pgmap"]["promote_op_per_sec"]
            ops_list.append(int(pps))
        else:
            pps = 0
             
        if arg == "ops":
            return sum(ops_list)
        elif arg == "rps":
            return rps
        elif arg == "wps":
            return wps
        else:
            return 0

    def get_cluster_total_pools(self):
        cluster_total_pools = commands.getoutput('timeout 10 ceph osd lspools  -f json-pretty 2>/dev/null')
        json_str = json.loads(cluster_total_pools)
        return len(json_str)

    def get_cluster_pools(self):
        '''get all pool name
        '''
        pool_list=[]
        data_dic = {}
        cluster_pools = commands.getoutput('timeout 10 ceph df -f json-pretty 2>/dev/null')
        json_str=json.loads(cluster_pools)
        for item in json_str["pools"]:
            pool_dic = {}
            pool_dic['{#POOLNAME}'] = str(item["name"])
            pool_list.append(pool_dic)
        data_dic['data'] = pool_list
        return json.dumps(data_dic,indent=4,separators=(',', ':'))

    def get_mds_subdirs(self):
        '''get all fs_sub_dir name
        '''
        try:
            #get fs_sub_dir rootdir:/mnt/cephfs  limit 200
            args = "timeout 10 ls -l /mnt/cephfs |grep '^d' | awk '{print $9}' | head -200" 
            t=subprocess.Popen(args,shell=True,stdout=subprocess.PIPE).communicate()[0]
            subdirs = [{'{#FSDIR_NAME}': dir} for dir in t.split('\n') if len(dir) != 0 ]

            return json.dumps({'data':subdirs},indent=4,separators=(',',':'))
        except:
            return json.dumps({'data':[]},indent=4,separators=(',',':'))

    def get_host_osds(self):
        try:
            osd_list=[]
            data_dic={}
            osds=[]
            host_osds = commands.getoutput("mount|grep osd|awk '{print $3}'|cut -f2 -d - 2>/dev/null")
            host_osds = host_osds.splitlines()
            for osd in host_osds:
                osd_dic = {}
                osd_dic['{#OSD}'] = str(osd)
                osd_list.append(osd_dic)
            data_dic['data'] = osd_list
            return json.dumps(data_dic,separators=(',', ':'))
        except:
            return json.dumps({'data':[]},indent=4,separators=(',',':'))

    def get_osd_mem_virt(self,osd,memtype):
        pidfile="/var/run/ceph/osd.{0}.pid".format(osd)
        osdpid = commands.getoutput('cat {0}  2>/dev/null'.format(pidfile))
        if not osdpid :
            return 0
        elif memtype == "virt":
            osd_runmemvsz = commands.getoutput('ps -p {0}  -o vsz |grep -v VSZ 2>/dev/null'.format(osdpid))
            return osd_runmemvsz
        elif memtype == "res":
            osd_runmemrsz = commands.getoutput('ps -p {0}  -o rsz |grep -v RSZ 2>/dev/null'.format(osdpid))
            return osd_runmemrsz

    def get_osd_cpu(self,osd):
        pidfile="/var/run/ceph/osd.{0}.pid".format(osd)
        osdpid = commands.getoutput('cat {0}  2>/dev/null'.format(pidfile))
        if not osdpid :
            return 0
        osd_cpu = commands.getoutput('''ps -p {0}  -o pcpu |grep -v CPU|awk 'gsub(/^ *| *$/,"")' 2>/dev/null'''.format(osdpid))
        return osd_cpu

    def get_pool_df(self,poolname,arg):
        json_str = self.loadData(self.ceph_df_file)

        pool_lst = [ item['name'] for item in json_str["pools"] ]
        if not poolname in pool_lst:
            raise Exception("Error ENOENT: unrecognized pool {0}".format(poolname))

        if arg == "used":
            for item in json_str["pools"]:
                if item["name"] == poolname:
                    return item["stats"]["bytes_used"]
        else:
            for item in json_str["pools"]:
                if item["name"] == poolname:
                    return item["stats"][arg]



    def get_pool_io_rate(self,poolname,stats):
        '''get every pool throughput,ops
        '''
        json_str = self.loadData(self.ceph_pool_state_file)
        if stats == "write":
            for item in json_str:
                if item["pool_name"] == poolname:
                    if item["client_io_rate"].has_key('write_bytes_sec') == True:
                        return  item["client_io_rate"]["write_bytes_sec"]
                    else:
                        return 0
        elif stats == "read":
            for item in json_str:
                if item["pool_name"] == poolname:
                    if item["client_io_rate"].has_key('read_bytes_sec') == True:
                        return item["client_io_rate"]["read_bytes_sec"]
                    else:
                        return 0
        elif stats == "op_write":
            for item in json_str:
                if item["pool_name"] == poolname:
                    if item["client_io_rate"].has_key('write_op_per_sec') == True:
                        return item["client_io_rate"]["write_op_per_sec"]
                    else:
                        return 0
        elif stats == "op_read":
            for item in json_str:
                if item["pool_name"] == poolname:
                    if item["client_io_rate"].has_key('read_op_per_sec') == True:
                        return item["client_io_rate"]["read_op_per_sec"]
                    else:
                        return 0
        else:
            for item in json_str:
                if item["pool_name"] == poolname:
                    if item["client_io_rate"].has_key(stats) == True:
                        return item["client_io_rate"][stats]
                    else:
                        return 0

    def get_pool_config(self,poolname,config):
        '''get cluster pool config
        '''
        if config == "id":
            pool_id = commands.getoutput("timeout 10 ceph   osd pool get {0} size -f json-pretty 2>/dev/null".format(poolname))
            json_str = json.loads(pool_id)
            return json_str["pool_id"]
        else:
            pool_cmd = commands.getoutput("timeout 10 ceph osd pool get {0} {1} -f json-pretty 2>/dev/null".format(poolname,config))
            json_str = json.loads(pool_cmd)
            return json_str[config]

    def get_rgw_bucket_stats(self,config):
        '''get cluster rgw bucket stats
        '''
        json_str = self.loadData(self.ceph_rgw_bucket_state_file)

        count_list = []
        if config == 'max_shard':
            arg = 'objects_per_shard'
        elif config == 'max_bucket':
            arg = 'num_objects'
        else:
            return "Wrong parameter!"
        for check in json_str:
            for bucket_data in check['buckets']:
                count_list.append(bucket_data[arg])

        return max(count_list)

    def get_fsdir_config(self,fsdir_name,config):
        rootdir = "/mnt/cephfs"
        if config == "fsdir_max_bytes":
            args = 'timeout 10 /usr/bin/getfattr -n ceph.quota.max_bytes {0} |grep "ceph.quota.max_bytes"|grep -o "[0-9]*"'.format(fsdir_name)
            p = subprocess.Popen(args, shell=True, cwd=rootdir,stdout=subprocess.PIPE,stderr=subprocess.PIPE).communicate()
            if p[0]:
                value = p[0].strip('\n')
            elif 'No such attribute' in p[1]:
                value = 0
            else:
                value = 'null'
            return value
        elif config == "fsdir_used":
            args = 'timeout 10 /usr/bin/getfattr -d -m ceph.dir.rbytes {0} |grep "ceph.dir.rbytes"| grep -o "[0-9]*"'.format(fsdir_name)
            p = subprocess.Popen(args, shell=True, cwd=rootdir,stdout=subprocess.PIPE,stderr=subprocess.PIPE).communicate()
            if p[0]:
                value = p[0].strip('\n')
            else:
                value = 'null'
            return value

def main():
    parser = argparse.ArgumentParser(description='ceph state', usage='%(prog)s [options]')
    parser.add_argument('-v','--version', action='version', version='%(prog)s 1.0')

    parser.add_argument('-k','--keys',nargs='+',dest='keys',metavar=('{ key1 }'),help='key')
    parser.add_argument('-p','--poolname',nargs=1,dest='poolname',metavar=('{ pool_name }'),help='poolname')
    
    if len(sys.argv)==1:
        print(parser.print_help())
    else:
        args=parser.parse_args()
        cephstate = CephState()
        if args.poolname:
            poolname = args.poolname[0]
            if poolname in ["lists","list"]:
                print(cephstate.get_cluster_pools())
                sys.exit(0)
            if args.keys:
                if len(args.keys) == 2:
                    if args.keys[0] == 'config':
                        print(cephstate.get_pool_config(poolname,args.keys[1]))
                    elif args.keys[0] == 'io':
                        print(cephstate.get_pool_io_rate(poolname,args.keys[1]))
                    elif args.keys[0] == 'df':
                        print(cephstate.get_pool_df(poolname,args.keys[1]))
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
                ops_list = ['ops','rps','wps']
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
