import ConfigParser
from libcloud.compute.types import Provider
from libcloud.compute.providers import get_driver
from libcloud.compute.deployment import ScriptDeployment

class EC2Binding:
    ''' EC2 binding for this distributed execution system '''

    def start_instance(self, instance_type, script):
      
      EC2_ACCESS_ID = self.accesskey
      EC2_SECRET = self.secretkey
      
      Driver = get_driver(Provider.EC2_US_EAST)
      conn = Driver(EC2_ACCESS_ID, EC2_SECRET)

      image = [i for i in conn.list_images() if i.id == self.ami ][0]
      size = [s for s in conn.list_sizes() if s.id == instance_type ][0]
      securitygroup = self.securitygroup
      conn.ex_authorize_security_group_permissive(securitygroup)


      key_name = 'gigg'

      locations = conn.list_locations()
      for location in locations:
        if location.availability_zone.name == 'us-east-1d':
          break;

      node = conn.create_node(name="node", location=location, image=image, size=size, ex_keyname = key_name, ex_securitygroup = securitygroup, ex_userdata=script)

      return node


    def end_instance(self, instance):

      EC2_ACCESS_ID = self.accesskey
      EC2_SECRET = self.secretkey

      Driver = get_driver(Provider.EC2_US_EAST)
      conn = Driver(EC2_ACCESS_ID, EC2_SECRET)

      conn.destroy_node(instance)
       

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
           
