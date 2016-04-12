CREATE DATABASE IF NOT EXISTS `api_log` DEFAULT CHARACTER SET UTF8 COLLATE utf8_general_ci;

GRANT ALL ON api_log.* TO 'indigo'@'%' IDENTIFIED BY 'indigopwd';
GRANT ALL ON api_log.* TO 'indigo'@'localhost' IDENTIFIED BY 'indigopwd';

--1 GET,2 POST,3 PUT,4 DELETE
--field_event_date,field_event_time,field_api,field_appid,field_userid,field_deviceid,field_result_code,field_latency,field_method,field_service
CREATE TABLE `log_indigo_request_test`(
`id` INT(9) AUTO_INCREMENT,
`event_date` DATE,
`event_time` TIME,
`api` VARCHAR(255),
`appid` VARCHAR(50),
`userid` VARCHAR(50),
`deviceid` VARCHAR(50),
`result_code` INT(3),
`latency` INT(9),
`method` TINYINT(1),
`service` VARCHAR(20),
PRIMARY KEY(id)
)ENGINE=Innodb DEFAULT CHARSET=utf8;

Totally 1365 files processed,total lines:23014009

Totally 1365 files processed,total lines:23040980
total spent 3118899

Totally 5623 files processed,total lines:51725009
total spent 2922460
