import os, inspect
from twisted.internet import reactor, threads, task, defer
from twisted.internet.defer import DeferredLock
from twisted.spread import pb, flavors, banana

class Job(flavors.RemoteCopy, flavors.Copyable):
    ''' Job definition '''
    
    def __init__(self, command, job_id, status = None):
        self.command = command
        self.job_id = job_id
        if status == None:
            self.status = 'ready'
        else:
            self.status = status

class ReceiverJob(flavors.RemoteCopy, flavors.Copyable):
    pass

flavors.setUnjellyableForClass(Job, ReceiverJob)
flavors.setUnjellyableForClass(ReceiverJob, Job)

class NodeInfo(flavors.RemoteCopy, flavors.Copyable):
    ''' Node information '''
    
    def __init__(self, instance_type = None, node_id = None):
        self.instance_type = instance_type
        self.node_id = node_id
        self.connected = True
        self.cur_jobs = 0

class ReceiverNodeInfo(flavors.RemoteCopy, flavors.Copyable):
    pass

flavors.setUnjellyableForClass(NodeInfo, ReceiverNodeInfo)
flavors.setUnjellyableForClass(ReceiverNodeInfo, NodeInfo)

class MessageObject(flavors.RemoteCopy, flavors.Copyable):
    def __init__(self, command, job = None, node = None):
        self.command = command
        self.job = job
        self.node = node

class ReceiverMessage(flavors.RemoteCopy, flavors.Copyable):
    pass

flavors.setUnjellyableForClass(MessageObject, ReceiverMessage)
flavors.setUnjellyableForClass(ReceiverMessage, MessageObject)

class CustomClientFactory(pb.PBClientFactory):
    
    def clientConnectionFailed(self, connector, reason):
        print 'Connection failed'
        try:
            reactor.stop()
        except:
            pass
    
    def clientConnectionLost(self, connector, reason):
        print 'Connection lost'
        try:
            reactor.stop()
        except:
            pass

class ClientMechanism(pb.Referenceable):

    def job_done(self, msg):
        self.server.callRemote("message", msg, self)

    def remote_message(self, msg):
        print('job received {0}'.format(msg.job.job_id))
        d = threads.deferToThread(self.client.execute, msg)
        d.addCallback(self.job_done)

    def __init__(self, client):
        self.client = client
        self.server = None

class ServerMechanism(pb.Root, object):
    
    def generate_client_script(self, instance_type, node_id):
        ''' Script for uploading the client code and starting it '''
    
        client = os.path.join(self.path, 'worker.py')
        proto = os.path.join(self.path, 'proto.py')
        with open(client, 'r') as content_file:
                content_client = content_file.read()
        content_client = content_client.encode("hex")
        
        with open(proto, 'r') as content_file:
                content_proto = content_file.read()
        content_proto = content_proto.encode("hex")

        script = '#!/bin/bash\n' \
                 + 'echo ' + content_client \
                 + ' | xxd -r -p > /root/worker.py\n' \
                 + 'echo ' + content_proto  \
                 + ' | xxd -r -p > /root/proto.py\n' \
                 + 'python /root/worker.py ' \
                 + self.ip + ' ' + self.port + ' ' + instance_type + ' ' \
                 + str(node_id) + ' ' + self.worker_log_dir \
                 + ' &>/root/worker_log\n'
        return script


    def schedule(self, node, job, nodes):
        raise Exception('Scheduling routine should be overridden')

    def start_server(self):
        reactor.listenTCP(int(self.port), pb.PBServerFactory(self))
        reactor.run()

    def schedules_ready(self, results):
        for node in results:
            for job in results[node]:
                if node.connected:
                    msg = MessageObject('job', job, node)
                    try:
                        self.connections[node.node_id].callRemote(
                                "message", msg)
                    except pb.DeadReferenceError:
                        print('Node id {0} is disconnected'.
                                format(node.node_id))
                        node.connected = False
            if node.connected == False:
                del(nodes[node.node_id])
                del(connections[node.node_id])
        self.lock.release()

    def start_scheduling(self, lock, msg):
        d = threads.deferToThread(self.schedule, msg.node, msg.job, 
                self.nodes)
        d.addCallback(self.schedules_ready)

    def remote_message(self, msg, client_connection):
        if(msg.command == 'info'):
            node = NodeInfo(msg.node.instance_type, msg.node.node_id)
            self.nodes[node.node_id] = node
            self.connections[node.node_id] = client_connection
        d = self.lock.acquire()
        d.addCallback(self.start_scheduling, msg)

    def __init__(self, ip, port, bot, worker_log_dir):
        self.ip = ip
        self.port = port
        self.bot = bot
        self.worker_log_dir = worker_log_dir
        self.path = os.path.dirname(os.path.abspath(inspect.getfile(
            inspect.currentframe())))
        self.connections = {}
        self.nodes = {}
        self.lock = DeferredLock()
