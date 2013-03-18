#!/usr/bin/python

import sys, ConfigParser
from scheduler import Scheduler
from proto import Job

def main(argv=None):
    if argv is None:
        argv = sys.argv
    
    config = ConfigParser.ConfigParser()
    config.read('config.ini')
    try:
        server_ip = config.get('system', 'host')
        server_port = config.get('system', 'port')
        instance_type = config.get('system', 'instance_type')
        instance_num = config.get('system', 'instance_num')
        jobs_log_dir = config.get('system', 'jobs_log_dir')
        bot_file = config.get('system', 'bot_file')
    except ConfigParser.NoOptionError:
        raise Exception('[-] missing config item in the "system" section')
    
    # Bag of tasks is in the form of {job_id: Job}
    bot = {}
    
    lines = open(bot_file).readlines()
    for i in range(0, len(lines)):
        bot[i] = Job(lines[i].strip(), i)
    
    scheduler = Scheduler(bot, instance_num, instance_type, server_ip, 
            server_port, jobs_log_dir)

if __name__ == "__main__":
    sys.exit(main())

