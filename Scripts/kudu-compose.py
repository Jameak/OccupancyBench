import argparse

SERVER_IP = "YOUR_SERVER_IP_HERE"
# These are all the default subnets. Add yours at the end if needed.
TRUSTED_SUBNETS = "127.0.0.0/8,10.0.0.0/8,172.16.0.0/12,192.168.0.0/16,169.254.0.0/16"

parser = argparse.ArgumentParser(description='Generate a docker-compose file for Kudu with the specified number of tablet servers')
parser.add_argument('scale', help='The number of tablet servers to create')
args = vars(parser.parse_args())

scale = int(args['scale'])

if(scale < 1):
    print("Scale must be >= 1")
    exit()


output = """
version: "3"
services:
  kudu-master:
    image: apache/kudu:1.11.1
    ports:
      - "7051:7051"
      - "8051:8051"
    command: ["master"]
    environment:
      - >
        MASTER_ARGS=--fs_wal_dir=/var/lib/kudu/master
        --rpc_bind_addresses=0.0.0.0:7051
        --rpc_advertised_addresses=""" + SERVER_IP + """:7051
        --webserver_port=8051
        --webserver_advertised_addresses=""" + SERVER_IP + """:8051
        --webserver_doc_root=/opt/kudu/www
        --stderrthreshold=0
        --use_hybrid_clock=false
        --trusted_subnets=""" + TRUSTED_SUBNETS + """
        --default_num_replicas=1"""

port = 7050
for i in range(0, scale):
    # This could be improved with f-strings, but they were introduced in python 3.6 and the version in my distribution is 3.5 ...
    output += '''
  kudu-tserver-''' + str(i) + ''':
    image: apache/kudu:1.11.1
    depends_on:
      - kudu-master
    ports:
      - "''' + str(port) + ''':''' + str(port) + '''"
      - "''' + str(port+2) + ''':''' + str(port+2) + '''"
    command: ["tserver"]
    environment:
      - KUDU_MASTERS=kudu-master:7051
      - >
        TSERVER_ARGS=--fs_wal_dir=/var/lib/kudu/tserver
        --rpc_bind_addresses=0.0.0.0:''' + str(port) + '''
        --rpc_advertised_addresses=''' + SERVER_IP + ''':''' + str(port) + '''
        --webserver_port=''' + str(port+2) + '''
        --webserver_advertised_addresses=''' + SERVER_IP + ''':''' + str(port+2) + '''
        --webserver_doc_root=/opt/kudu/www
        --stderrthreshold=0
        --use_hybrid_clock=false'''
    
    port += 10
    
print(output)