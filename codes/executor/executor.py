from datetime import datetime
import socket
import subprocess
import time
import pymysql.cursors

import constants


class MySQL():

    def __init__(self):
        self.connection = pymysql.connect(host=constants.MYSQL_HOST,
                                     user=constants.MYSQL_USER,
                                     password=constants.MYSQL_PASSWORD,
                                     db=constants.MYSQL_DB,
                                     cursorclass=pymysql.cursors.DictCursor)


    def update_task_status(self, task_id, status):
        with self.connection.cursor() as cursor:
            sql = ''
            if status == 0:
                sql = 'UPDATE task_details SET execution_start_time=%s where task_id=%s'
            else:
                sql = 'UPDATE task_details SET execution_end_time=%s where task_id=%s'
            cursor.execute(sql, (datetime.now(), task_id))
        self.connection.commit()

    def update_executor_status(self, server_id, status):
        with self.connection.cursor() as cursor:
            sql = ''
            if status == 0:
                sql = 'UPDATE client_details SET status=1 WHERE server_id=%s'
                cursor.execute(sql, (server_id))
            else:
                sql = 'UPDATE client_details SET status=0, last_job_end_time=%s WHERE server_id=%s'
                cursor.execute(sql, (datetime.now(), server_id))
        self.connection.commit()


def get_data_from_schedular():
    host = ''
    port = constants.CLIENT_PORT

    mySocket = socket.socket()
    mySocket.bind((host,int(port)))

    data = ''
    mySocket.listen(1)
    conn, addr = mySocket.accept()
    print("Connection from: " + str(addr))
    while True:
        data = conn.recv(4096).decode()
        print ("from connected  user: " + str(data))
        conn.send(b'1')
        conn.close()
        break
    return data

def process_task(commands):
    """execute the task and return the task status

    the command is expected to be in the format
    [task_id] convert [url] [server_id]

    Arguments:
        command_from_schedular string
    """
    mysql_client.update_task_status(int(commands[0]), 0)
    # run ffmpeg for this file
    fileUrl = commands[2]
    command = 'ffmpeg -i "{}"  {}.mp3'.format(commands[2], commands[0])
    print(command)
    subprocess.call(command, shell=True)
    mysql_client.update_task_status(int(commands[0]), 1)


if __name__ == '__main__':
    mysql_client = MySQL()
    while True:
        time.sleep(5)
        command_from_schedular = get_data_from_schedular()
        commands = command_from_schedular.split()
        mysql_client.update_executor_status(commands[3], 0)
        process_task(commands)
        mysql_client.update_executor_status(commands[3], 1)