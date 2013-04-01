from twisted.internet import reactor
from twisted.internet.protocol import Factory, Protocol
from proto import *
import sys, os, errno, subprocess
from subprocess import Popen
from libcloud.storage.providers import get_driver
from libcloud.storage.types import Provider, ContainerDoesNotExistError

class Client:
    
    def mkdir(self, path):
        try:
            os.makedirs(path)
        except OSError as e:
            if e.errno == errno.EEXIST and os.path.isdir(path):
                pass
            else:
                raise

    def dump_streams(self, out, err, msg):
        config = ConfigParser.ConfigParser()
        config.read('config.ini')
        try:
            self.accesskey = config.get('ec2', 'accesskey')
            self.secretkey = config.get('ec2', 'secretkey')
        except ConfigParser.NoOptionError:
            raise Exception('[-] missing config item in the "ec2" section')


        Driver = get_driver(Provider.EC2_US_EAST)
        conn = Driver(self.acceskey, self.secretkey)

        jobfolder = os.path.join(self.base, str(msg.job.job_id))
        self.mkdir(jobfolder)
        f = open(os.path.join(jobfolder, 'out'), 'w')
        f.write(out)
        f.close()
        f = open(os.path.join(jobfolder, 'err'), 'w')
        f.write(err)
        f.close()
        f = open(os.path.join(jobfolder, 'param'), 'w')
        f.write(msg.job.command)
        f.close()

        
        container = conn.create_container("taskfarm");
        conn.upload_object(jobfolder, container, str(msg.job.job_id);


    def execute(self, msg):
        p = Popen(msg.job.command, stdout=subprocess.PIPE, 
                stderr=subprocess.PIPE, shell = True)
        out, err = p.communicate()
        self.dump_streams(out, err, msg)
        return msg

    def __init__(self, instance_type, node_id, log_dir):
        self.instance_type = instance_type
        self.base = log_dir
        self.node_id = node_id

def got_server_connection(server, client_mechanism):
    client_mechanism.server = server
    node = NodeInfo(client_mechanism.client.instance_type,
            client_mechanism.client.node_id)
    msg = MessageObject('info', None, node)
    server.callRemote("message", msg, client_mechanism)
    

def main(argv=None):
    if argv is None:
        argv = sys.argv
    if len(argv) != 6:
        print('Usage: {0} server_ip server_port instance_type node_id log_dir'
                .format(argv[0]))
        return -1
    
    client = Client(argv[3], argv[4], argv[5])
    client_mech = ClientMechanism(client)

    f = CustomClientFactory()
    reactor.connectTCP(argv[1], int(argv[2]), f)
    d = f.getRootObject()
    d.addCallback(got_server_connection, client_mech)
    reactor.run()

if __name__ == "__main__":
    sys.exit(main())

