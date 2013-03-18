import ConfigParser
from libcloud.compute.types import Provider
from libcloud.compute.providers import get_driver

class EC2Binding:
    ''' EC2 binding for this distributed execution system '''

    def start_instance(self, instance_type, script):
      
      EC2_ACCESS_ID = self.accesskey
      EC2_SECRET = self.secretkey
      
      Driver = get_driver(Provider.EC2_US_EAST)
      conn = Driver(EC2_ACCESS_ID, EC2_SECRET)

      image = [i for i in conn.list_images() if i.id == self.ami ][0]
      size = [s for s in conn.list_sizes() if s.id == instance_type ][0]

      key_name = 'gigg'

      key_path = '/root/gigg.pem'


      node = conn.deploy_node(name='test', image=image, size=size, deploy=script,ssh_username='root', ssh_key=key_path,ex_keyname=key_name)

      return node


    def end_instance(self, instance):

      EC2_ACCESS_ID = '9574-1557-8438'
      EC2_SECRET = '58C2nGy1Qf/ysMJepLjuYK2fDOrQGByxlktaMG8D'

      Driver = get_driver(Provider.EC2_US_EAST)
      conn = Driver(EC2_ACCESS_ID, EC2_SECRET)

      destroy_node(instance)
       

    def __init__(self):
        
        # reading the general ec2 config
        config = ConfigParser.ConfigParser()
        config.read('config.ini')
        try:
            self.accesskey = config.get('ec2', 'accesskey')
            self.secretkey = config.get('ec2', 'secretkey')
            self.keypair = config.get('ec2', 'keypair')
            self.securitygroup = config.get('ec2', 'securitygroup')
            self.ami = config.get('ec2', 'ami')
        except ConfigParser.NoOptionError:
            raise Exception('[-] missing config item in the "ec2" section')
           
