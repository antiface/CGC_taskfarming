from proto import Job, NodeInfo, ServerMechanism
from ec2_binding import EC2Binding
from datetime import datetime
from twisted.internet import reactor, threads
import sys, os, signal

class Scheduler(ServerMechanism):

    def schedule(self, node, job, nodes):
        ''' Scheduling routine
        
        This function is invoked when either:
        1) a node is booted up (job = None)
        2) a job on a node is finished

        Input:
        'node'  NodeInfo    : The node which has a pending event
        'job'   Job         : If set, the current finished job
        'nodes' [NodeInfo]  : Current active nodes in the system
    
        Output:
        This function should return a dictionary {node:[job]} which defines 
        new jobs for different nodes. Note that the job execution can fail.
        This function is able to detect possible failures by following 
        job.status.
        '''
        
        # Node connected?
        if job == None:
            print('Worker with Node ID {0} connected'.format(node.node_id))
        # Job done.
        else:
            self.bot[job.job_id].status = 'done'

        jobs_ready = [job for job_id, job in self.bot.iteritems() 
                if job.status == 'ready']
        jobs_done = [job for job_id, job in self.bot.iteritems() 
                if job.status == 'done']
        jobs_exec = [job for job_id, job in self.bot.iteritems()
                if job.status == 'exec']

        # All jobs done?
        if len(jobs_done) == len(self.bot):
            print('All jobs done. Shutting down VMs.')
            self.finalize(self.instances)
            print('Done.')
            delta = datetime.now() - self.before
            print ('Took {0} seconds').format(delta.seconds)
            os.kill(os.getpid(), signal.SIGINT)

        # Naive scheduling
        schedules = {}
        joblist = [];
        if len(jobs_ready) != 0:
	        for i in range(len(jobs_ready)):
            job = jobs_ready[i]
            job.status = 'exec'
            joblist.append(job);

        schedules[node] = joblist
        
        jobs_size = 0
        print_str = 'Schedules:\n'
        for node, jobs in schedules.iteritems():
            print_str += 'Node ' + node.node_id + ':\n'
            for job in jobs:
                print_str += ' Job id {0}\n'.format(job.job_id)
                jobs_size += 1
        if (jobs_size != 0):
            print('{0}'.format(print_str))
        
        return schedules

    def start_vms(self, vm_num):
        
        self.ec2 = EC2Binding()
        for i in range(int(vm_num)):
            start_up_script = self.generate_client_script(self.instance_type, i)
            instance = self.ec2.start_instance(self.instance_type, 
                    start_up_script)
            self.instances.append(instance)

    def __init__(self, bot, vm_num, instance_type, ip, port, worker_log_dir):

        super(Scheduler, self).__init__(ip, port, bot, worker_log_dir)

        self.before = datetime.now()
        self.bot = bot
        self.vm_num = vm_num
        self.instance_type = instance_type
        self.instances = []
        
        # Start up the VMs
        threads.deferToThread(self.start_vms, vm_num)

        # Start up the server
        self.start_server()
 
    def finalize(self, instances):

        for instance in instances:
            self.ec2.end_instance(instance)

