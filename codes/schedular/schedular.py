"""File containing."""
from datetime import datetime
import socket
import threading
import time
import paramiko
import pika
import pymysql.cursors
from novaclient import client as nova
from neutronclient.v2_0 import client as neutron
from keystoneauth1 import session
from keystoneauth1 import loading

import constants

class MySQL:

    def __init__(self):
        self.connection = pymysql.connect(host=constants.MYSQL_HOST,
                                     user=constants.MYSQL_USER,
                                     password=constants.MYSQL_PASSWORD,
                                     db=constants.MYSQL_DB,
                                     cursorclass=pymysql.cursors.DictCursor)

    def insert_task(self, task_details):
        id_of_new_row = 0
        with self.connection.cursor() as cursor:
            insert_statement = 'INSERT INTO task_details(task_details, time_added) values (%s,%s)'
            cursor.execute(insert_statement, (task_details, datetime.now()))
            id_of_new_row = cursor.lastrowid
        self.connection.commit()
        return id_of_new_row

    def get_free_clients(self):
        """

        status 0 = Idle
               1 = Busy
        """
        list_of_clients = []
        with self.connection.cursor(pymysql.cursors.DictCursor) as cursor:
            query = 'SELECT client_id, client_ip, client_port, server_id from client_details where status=0'
            cursor.execute(query)
            for row in cursor:
                client = {
                    'client_id': row['client_id'],
                    'client_ip': row['client_ip'],
                    'client_port': row['client_port'],
                    'server_id': row['server_id'],
                }
                list_of_clients.append(client)
        return list_of_clients

    def insert_client_details(self, ip, server_id):
        """

        [description]

        Arguments:
            ip {[type]} -- [description]
        """
        with self.connection.cursor() as cursor:
            query = 'INSERT INTO client_details(CLIENT_IP, CLIENT_PORT, SERVER_ID, START_TIME, STATUS) value (%s,%s,%s,%s,%s)'
            cursor.execute(query, (ip, constants.CLIENT_PORT,server_id,datetime.now(), 0))
        self.connection.commit()

    def get_long_time_idle_clients(self):
        idle_clients = []
        with self.connection.cursor() as cursor:
            query = 'SELECT * FROM client_details'
            cursor.execute(query)
            for row in cursor:
                last_job_end = row['last_job_end_time']
                now = datetime.now()
                if now - last_job_end > constants.VM_MAX_IDLE_TIME:
                    idle_clients.append(row['sever_id'])
        return idle_clients

class OpenStackClient():

    def __init__(self):
        loader = loading.get_plugin_loader(constants.OPENSTACK_PASSWORD)
        auth = loader.load_from_options(auth_url=constants.OPENSTACK_URL,username=constants.OPENSTACK_USER,
            password=constants.OPENSTACK_PASSWORD,project_id=constants.OPENSTACK_PROJECT_ID)
        self.sess = session.Session(auth=auth)

    def get_available_floating_ip(self):
        """Return the first encountered floating ip

        [description]

        Returns:
            [type] -- [description]
        """
        neutron_client = neutron.Client(session=self.sess)
        floating_ips = neutron_client.list_floatingips()
        for ip in floating_ips['floatingips']:
            if ip['status'] == 'DOWN':
                return ip['floating_ip_address']

    def create_server(self):
        """Create a server with an image that has client deployed on it

        :return : floating ip
        """
        available_floating_ip = ''
        with nova.Client(constants.OPENSTACK_VERSION, session=self.sess) as client:
            flavor = client.flavors.find(name='m1.small')
            image = client.glance.find_image('executor')
            server = client.servers.create('server{}'.format(datetime.now()), image, flavor)
            time.sleep(10)
            available_floating_ip = self.get_available_floating_ip()
            server.add_floating_ip(available_floating_ip)
        return available_floating_ip, server._info['id']

def listen_for_tasks():
    connection = pika.BlockingConnection(pika.ConnectionParameters(constants.RABBITMQ_SERVER))
    channel = connection.channel()
    channel.basic_consume(on_message, 'tasks')
    try:
        print('Started consuming')
        channel.start_consuming()
    except KeyboardInterrupt:
        channel.stop_consuming()
    connection.close()



def on_message(channel, method_frame, header_frame, body):
    channel.basic_ack(delivery_tag=method_frame.delivery_tag)
    handle_workflow(body)


def handle_workflow(body):
    """Method to handle the whole workflow of sending tasks to client
    and fetching the responses

    :param body: message received from queue
    """
    print(body)
    task_id = mysql_client.insert_task(body)
    #check for available workers
    clients = mysql_client.get_free_clients()
    if len(clients) > 0:
        # clients are free, send tasks
        client = clients[0]
        send_task(task_id, client['client_ip'], constants.CLIENT_PORT, body, client['server_id'])
    else:
        # start client
        # openstack code goes herei
        openstack_client = OpenStackClient()
        ip, server_id = openstack_client.create_server()
        mysql_client.insert_client_details(ip, server_id)
        time.sleep(60)
        send_task(task_id, ip, constants.CLIENT_PORT, body, server_id)

def send_task(task_id, ip, port, body, server_id):
    """Method to send task to client

    Arguments:
        task_id - task id inserted in the database
        ip IP - Clients ip
        port client port
        body command to send
    """
    #print('{} - {}:{} {}'.format(task_id, ip, port, body))
    counter = 4
    while counter > 0:
        try:
            #time.sleep(600)
            #ssh = paramiko.SSHClient()
            #ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            #ssh.connect(ip, username='ubuntu', key_filename='/home/prajakt/.ssh/id_rsa')
            #stdin, stdout, stderr = ssh.exec_command('/usr/bin/python /home/ubuntu/executor.py')
            #print(stdout)
            #time.sleep(60)i
            #print('ssh sent')
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect((ip, int(port)))
            s.sendall(str.encode('{} {} {}'.format(task_id, body, server_id)))
            #block till data is not received
            data = s.recv(1024)
            s.close()
            break
        except Exception as e:
            print(e)
            counter -= 1


def worker():
    while True:
        # check for idle
        # if idle for more than 10 min
        # shut down server
        # and update record in database
        mysql = MySQL()
        openstack_client = OpenStackClient()
        list_of_clients = mysql.get_long_time_idle_clients()
        for client in list_of_clients:
            openstack_client.servers.force_delete(client)

if __name__ == '__main__':
    mysql_client = MySQL()
    #t = threading.Thread(target=worker)
    #t.daemon = True  # thread dies when main thread (only non-daemon thread) exits.
    #t.start()
    listen_for_tasks()
