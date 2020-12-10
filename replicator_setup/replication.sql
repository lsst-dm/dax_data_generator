SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 ;
SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 ;
SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='TRADITIONAL' ;

-----------------------------------------------------------
-- Preload configuration parameters for testing purposes --
-----------------------------------------------------------

-- Common parameters of all types of servers

INSERT INTO `config` VALUES ('common', 'request_buf_size_bytes',     '131072');
INSERT INTO `config` VALUES ('common', 'request_retry_interval_sec', '5');

-- Controller-specific parameters

INSERT INTO `config` VALUES ('controller', 'num_threads',         '2');
INSERT INTO `config` VALUES ('controller', 'http_server_port',    '25080');
INSERT INTO `config` VALUES ('controller', 'http_server_threads', '2');
INSERT INTO `config` VALUES ('controller', 'request_timeout_sec', '60');
INSERT INTO `config` VALUES ('controller', 'job_timeout_sec',     '60');
INSERT INTO `config` VALUES ('controller', 'job_heartbeat_sec',   '0');   -- temporarily disabled
INSERT INTO `config` VALUES ('controller', 'empty_chunks_dir',    '/home/jgates/qserv-run/var/lib/qserv');

-- Database service-specific parameters

INSERT INTO `config` VALUES ('database', 'services_pool_size',   '2');
INSERT INTO `config` VALUES ('database', 'qserv_master_host',    'localhost');
INSERT INTO `config` VALUES ('database', 'qserv_master_port',    '3306');
INSERT INTO `config` VALUES ('database', 'qserv_master_user',    'qsmaster');
INSERT INTO `config` VALUES ('database', 'qserv_master_name',    'qservMeta');
INSERT INTO `config` VALUES ('database', 'qserv_master_services_pool_size', '2');
INSERT INTO `config` VALUES ('database', 'qserv_master_tmp_dir', '/home/jgates/qserv-run/var/lib/ingest');

-- Connection parameters for the Qserv Management Services

INSERT INTO `config` VALUES ('xrootd', 'auto_notify',         '1');
INSERT INTO `config` VALUES ('xrootd', 'host',                'localhost');
INSERT INTO `config` VALUES ('xrootd', 'port',                '1094');
INSERT INTO `config` VALUES ('xrootd', 'request_timeout_sec', '60');

-- Default parameters for all workers unless overwritten in worker-specific
-- tables

INSERT INTO `config` VALUES ('worker', 'technology',                 'FS');
INSERT INTO `config` VALUES ('worker', 'svc_port',                   '26000');
INSERT INTO `config` VALUES ('worker', 'fs_port',                    '25001');
INSERT INTO `config` VALUES ('worker', 'num_svc_processing_threads', '2');
INSERT INTO `config` VALUES ('worker', 'num_fs_processing_threads',  '2');       -- double compared to the previous one to allow more elasticity
INSERT INTO `config` VALUES ('worker', 'fs_buf_size_bytes',          '4194304');  -- 4 MB
INSERT INTO `config` VALUES ('worker', 'data_dir',                   '/home/jgates/qserv-run/var/lib/mysql');
INSERT INTO `config` VALUES ('worker', 'db_port',                    '3306');
INSERT INTO `config` VALUES ('worker', 'db_user',                    'root');
INSERT INTO `config` VALUES ('worker', 'loader_port',                '25002');
INSERT INTO `config` VALUES ('worker', 'loader_tmp_dir',             '/home/jgates/qserv-run/var/lib/ingest');
INSERT INTO `config` VALUES ('worker', 'exporter_port',              '25003');
INSERT INTO `config` VALUES ('worker', 'exporter_tmp_dir',           '/home/jgates/qserv-run/var/lib/export');
INSERT INTO `config` VALUES ('worker', 'num_loader_processing_threads',   '2');
INSERT INTO `config` VALUES ('worker', 'num_exporter_processing_threads', '2');

INSERT INTO `config` VALUES ('worker','http_loader_port','25004');
INSERT INTO `config` VALUES ('worker','http_loader_tmp_dir','/home/jgates/qserv-run/var/lib/ingest');
INSERT INTO `config` VALUES ('worker','num_http_loader_processing_threads','2');

-- Preload parameters for runnig all services on the same host

INSERT INTO `config_worker` VALUES ('worker', 1, 0, 'localhost', NULL, 'localhost', NULL, NULL, 'localhost', NULL, NULL, '192.168.1.143', NULL, NULL, '192.168.1.143', NULL, NULL,'192.168.1.143', NULL, NULL);

-- This is the default database family for LSST production

INSERT INTO `config_database_family` VALUES ('production', 1, 340, 3, 0.01667);

SET SQL_MODE=@OLD_SQL_MODE ;
SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS ;
SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS ;
