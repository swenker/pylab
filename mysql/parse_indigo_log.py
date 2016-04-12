__author__ = 'wenjusun'

import MySQLdb
import gzip
import os

import time

REQUEST_LOG_TABLE='log_indigo_request_test2'

class IndigoLogHandler():


    def parse_files(self,folder):
        log_files = os.listdir(folder)
        file_counter = 0
        all_lines=0
        db = MySQLdb.connect(host="localhost",user="indigo",passwd="indigopwd",db="api_log",charset="utf8")
        try:
            cursor = db.cursor()

            for log_file in log_files:
                start_time=time.time()*1000

                tmp_counter=self.parse_indigo_log("%s/%s"%(log_folder,log_file),db,cursor)
                all_lines+=tmp_counter

                file_counter += 1
                end_time=time.time()*1000
                print "%d:%d--%d" %(file_counter,tmp_counter,end_time-start_time)

        except BaseException,e:
            print e
        finally:
            db.close()

        print 'Totally %d files processed,total lines:%d' %(file_counter,all_lines)


    def parse_indigo_log(self,log_file,db,cursor):

        gzip_file_handler = gzip.open(log_file)

        line_counter=0
        try:
            base_sqls= 'INSERT INTO '+REQUEST_LOG_TABLE+' (event_date,event_time,api,appid,userid,deviceid,result_code,latency,method,service)VALUES'
            value_sqls=''
            linestr = gzip_file_handler.readline()

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
                        field_result_code = self.grep_value(log_fileds[14])
                        field_latency = self.grep_value(log_fileds[15])
                        field_method = self.convert_method(self.grep_value(log_fileds[16]))
                        field_service = self.grep_value(log_fileds[17])

                        # print field_event_date,field_event_time,field_api,field_appid,field_userid,field_deviceid,field_result_code,field_latency,field_method,field_service

                        value_sqls+="('%s','%s','%s','%s','%s','%s',%s,%s,%s,'%s')," %(field_event_date,field_event_time,field_api,field_appid,field_userid,field_deviceid,field_result_code,field_latency,field_method,field_service)

                        if line_counter%6000==0:
                            final_sqls=base_sqls+value_sqls[0:len(value_sqls)-1]
                            cursor.execute(final_sqls)
                            db.commit()
                            value_sqls=''

                linestr = gzip_file_handler.readline()

            if value_sqls:
                final_sqls=base_sqls+value_sqls[0:len(value_sqls)-1]
                cursor.execute(final_sqls)
                db.commit()
                value_sqls=''

        except BaseException,e:
            print e
        finally:
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


if __name__ == '__main__':
    print 'hello'

    log_file = "/home/wenjusun/cloudservices-log/cloud-service1/cloud-service-1.0.log.2016-01-04-03.gz"
    log_folder = "/home/wenjusun/cloudservices-log/cloud-service2"

    start_time=time.time()*1000
    # print IndigoLogHandler().parse_indigo_log(log_file)
    IndigoLogHandler().parse_files(log_folder)
    end_time=time.time()*1000
    print "total spent %d" %(end_time-start_time)



