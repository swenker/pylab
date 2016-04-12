__author__ = 'wenjusun'

import gzip
import os
import logging
import time
from cassandra.cluster import BatchStatement
from cassandra_client import CassandraClient
from cassandra import util
from datetime import datetime

REQUEST_LOG_TABLE='log_indigo_request_test2'

log = logging.getLogger()
log.setLevel('INFO')


class IndigoLogHandler():
    cassandra_client = CassandraClient()

    def to_datetime(self,datestr,timestr):
        return datetime.strptime(datestr+timestr,"%Y-%m-%d%H:%M:%S")

    def init_schema(self):

            self.cassandra_session.execute("""CREATE KEYSPACE device_logs WITH replication = {'class':'SimpleStrategy', 'replication_factor':3};""")

            self.cassandra_session.execute("""
                CREATE TABLE device_logs.device_action (
                    id uuid PRIMARY KEY,
                    deviceId text,

                    items set<text>
                );
            """)
            self.cassandra_session.execute("""
                CREATE TABLE device_logs.log_entry (
                    id uuid PRIMARY KEY,
                    deviceId text,
                    eventDatetime timestamp,
                    api text,
                    appid text,
                    userid text,
                    resultCode int,
                    latency int,
                    service text,
                    method int
                );
            """)
            log.info('keyspace and schema created.')

    def parse_log_folders(self,folder):
        log_files = os.listdir(folder)
        file_counter = 0
        all_lines=0


        try:
            self.cassandra_client.connect(["s000.blurdev.com"])

            for log_file in log_files:
                start_time=time.time()*1000

                tmp_counter=self.parse_indigo_log_file("%s/%s"%(log_folder,log_file))
                all_lines+=tmp_counter

                file_counter += 1
                end_time=time.time()*1000
                print "%d:%d--%d" %(file_counter,tmp_counter,end_time-start_time)

        except BaseException,e:
            print e
        finally:
            self.cassandra_client.close()

        print 'Totally %d files processed,total lines:%d' %(file_counter,all_lines)


    # def parse_indigo_log_file(self,log_file,cassandra_session):
    def parse_indigo_log_file(self,log_file):
        self.cassandra_client.connect(["s000.blurdev.com"])
        cassandra_session = self.cassandra_client.session
        gzip_file_handler = gzip.open(log_file)

        line_counter=0
        try:

            linestr = gzip_file_handler.readline()
            pstmt = cassandra_session.prepare("INSERT INTO device_logs.log_entry(id,deviceId,eventDatetime,api,appid,userid,resultCode,latency,service,method)VALUES(?,?,?,?,?,?,?,?,?,?)")

            batch_statement = BatchStatement()
            while(linestr):

                #2016-01-04 03:00:00,472 +0000 [0:0:0:0:0:0:0:1] INFO  [qtp389572888-163610] com.motorola.blur.cloudsvc.service.CloudService#internalCrossZoneApiCall(1237) - [CloudService.Report.RemoteZone]:
                # api=/v1/dp/validatesession url=https://api-sg.svcmot.com/v1/dp/validatesession.json?_remotecall=1&_indigo_zone=CN&authtoken=0-fa644269f5406c77fb0143a35a9d265a1031705043&deviceid=1288446770950721536
                # result=400 time=599


                #-----use this one.
                #2016-01-04 03:59:59,293 +0000 [0:0:0:0:0:0:0:1] INFO  [qtp389572888-163690] com.motorola.blur.cloudsvc.service.CloudService#invoke(579) - [CloudService.Report.API]:
                # api=/v1/checkinuploader/upload appid=YDYWOLQB1NM35HHYPKOZW3V3Z33TC85I userid=null deviceid=1342508016933724160 status=200 time=1170 method=POST service=ccs_uploader
                # URI=/v1/checkinuploader/upload.pb querystring:deviceid=1342508016933724160&appId=YDYWOLQB1NM35HHYPKOZW3V3Z33TC85I&geolocation=China-East&geocheckintimestamp=1451879998028

                # print linestr
                if linestr.count('CloudService.Report.API')>0:
                    log_fileds = linestr.split(' ')
                    # print len(log_fileds)
                    # print log_fileds
                    if len(log_fileds) == 20:

                        line_counter+=1

                        field_event_date = log_fileds[0]
                        field_event_time = self.extract_time(log_fileds[1])
                        field_api = self.grep_value(log_fileds[10])
                        field_appid = self.grep_value(log_fileds[11])
                        field_userid = self.grep_value(log_fileds[12])
                        field_deviceid = self.grep_value(log_fileds[13])
                        field_result_code = self.get_int(self.grep_value(log_fileds[14]))
                        field_latency = self.get_int(self.grep_value(log_fileds[15]))
                        field_method = self.convert_method(self.grep_value(log_fileds[16]))
                        field_service = self.grep_value(log_fileds[17])

                        # print field_event_date,field_event_time,field_api,field_appid,field_userid,field_deviceid,field_result_code,field_latency,field_method,field_service

                        edt = self.to_datetime(field_event_date,field_event_time)
                        batch_statement.add(
                            pstmt.bind((util.uuid_from_time(edt),field_deviceid,edt,
                                       field_api,field_appid,field_userid,field_result_code,field_latency,field_service,field_method)))

                        if line_counter%6000==0:
                            cassandra_session.execute(batch_statement)
                            batch_statement = BatchStatement()

                linestr = gzip_file_handler.readline()

            if line_counter%6000!=0:
                cassandra_session.execute(batch_statement)


        except BaseException,e:
            print e
        finally:
            self.cassandra_client.close()
            if gzip_file_handler:
                gzip_file_handler.close()

        return line_counter

    def grep_value(self,key_value):
        if key_value!='':
            return self.normalize_string(key_value.split('=')[1])

    def extract_time(self,timestring):
        '03:59:58,169'
        return timestring[0:8]

    def convert_method(self,method_string):
        if not method_string:
            return 0
        elif 'GET' == method_string:
            return 1
        elif 'POST' == method_string:
            return 2
        elif 'PUT' == method_string:
            return 3
        elif 'DELETE' == method_string:
            return 4

    def normalize_string(self,value):
        if value == 'null':
            return ''
        else:
            return value
    def get_int(self,val):
        normalized_value=self.normalize_string(val)
        if normalized_value:
           return int(normalized_value)
        else:
           return 0

    def query_device_log(self,device_id):
        self.cassandra_client.connect(["s000.blurdev.com"])
        results = self.cassandra_client.session.execute("SELECT * FROM device_logs.log_entry WHERE deviceid='"+device_id+"' ")
        # results = self.cassandra_client.session.execute("SELECT * FROM device_logs.log_entry WHERE deviceid='1441518936169762816' ")
        for row in results:
            print row.latency,row.api


        self.cassandra_client.close()

if __name__ == '__main__':
    print 'hello'

    log_file = "/home/wenjusun/cloudservices-log/cloud-service1/cloud-service-1.0.log.2016-01-04-03.gz"
    log_folder = "/home/wenjusun/cloudservices-log/cloud-service2"

    start_time=time.time()*1000
    # print IndigoLogHandler().parse_indigo_log_file(log_file)
    # IndigoLogHandler().parse_files(log_folder)
    IndigoLogHandler().query_device_log('1441518936169762816')

    end_time=time.time()*1000
    print "total spent %d" %(end_time-start_time)



