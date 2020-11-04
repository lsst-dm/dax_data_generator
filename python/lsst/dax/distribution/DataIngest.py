#!/usr/bin/env python3

# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (http://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.


import json
import requests
import subprocess


class DataIngest():
    """This class is used to communicate with the ingest system using
    json formated requests and responses.

    Parameters
    ----------
    host : str
        Data ingest server host.
    port : int
        Data ingerst server port number.
    auth_key : str, optional
        Authorization key.
    """

    def __init__(self, host, port, auth_key=''):
        self._host = host
        self._port = port
        self._auth_key = auth_key
        self._base_url = 'http://' + self._host + ':' + str(port) + '/'
        print("base_url=", self._base_url)

    def __repr__(self):
        out = ("host=" + self._host + ":" + str(self._port) + " auth_key:****")
        return out

    def isIngestAlive(self):
        """Test if ingest system is alive.
        """
        success, status_code, r_json = self._requestToIngest("GET", 'meta/version', None)
        if not success:
            return False
        print('ingest version=', r_json['version'])
        return True

    def _requestToIngest(self, req_type, ingest_cmd, data_json):
        """Send the REST request to the ingest system.

        Parameters
        ----------
        req_type : str
            Indicates the type of rest action. It must be one of "PUT",
            "POST", or "GET"
        ingest_cmd : str
            The command for the ingest system
        data_json : json
            json object containing the data. It is not used for 'GET'.

        Return
        ------
        success : bool
            True indicates operation was successful
        status_code : int
            Status code value from put request.
        r_json : json or None
            If the status code was not 200, this will be None.
            Otherwise it is a json object with information about the request.

        """
        url = self._base_url + ingest_cmd
        print('url=', url, " data=", data_json)
        if req_type == "PUT":
            response = requests.put(url, json=data_json)
        elif req_type == "POST":
            response = requests.post(url, json=data_json)
        elif req_type == "GET":
            response = requests.get(url)
        else:
            raise ValueError(f"requestToIngest req_type must be one of PUT, POST, or GET. req={req_type}")
        status_code = response.status_code
        success = True
        # 200 means the put request was at least well formed.
        if status_code != 200:
            print('ERROR put url=', url, "data=", data_json, 'status=', status_code)
            success = False
            return success, status_code, None
        r_json = response.json()
        if not r_json['success']:
            print('ERROR put url=', url, 'status=', status_code, 'data=', data_json,
                  'r_json=', r_json)
            success = False
        return success, status_code, r_json

    def registerDatabase(self, db_file_path):
        """ Send the database description to the ingest system.
        """
        with open(db_file_path, 'r') as db_f:
            data = db_f.read()
            data_json = json.loads(data)
        success, status_code, r_json = self._requestToIngest("POST", 'ingest/database', data_json)
        if not success:
            print('ERROR while sending databaae ', db_file_path, "r_json=", r_json)
            return False
        return True

    def registerTable(self, schema_file_path):
        with open(schema_file_path, 'r') as schema_f:
            data = schema_f.read()
            data_json = json.loads(data)
        success, status_code, r_json = self._requestToIngest("POST", 'ingest/table', data_json)
        if not success:
            print('ERROR while sending table schema ', schema_file_path, "r_json=", r_json)
            return False
        return True

    def startTransaction(self, db_name):
        """ Start an ingest super transaction.

        Return
        ------
        success : bool
            True when transaction was started successfully.
        id : int
            Ingest super transaction id number.
        """
        success, status_code, r_json = self._requestToIngest("POST", 'ingest/trans',
                                       {'database': db_name, 'auth_key': self._auth_key})
        if not success:
            print('ERROR when starting transaction ', db_name, "r_json=", r_json)
            return False, -1
        id = r_json['databases'][db_name]['transactions'][0]['id']
        print("transaction id=", id, " r_json=", r_json)
        return True, id

    def endTransaction(self, transaction_id, abort):
        """ End the ingest transaction

        Parameters
        ----------
        transaction_id : int
            The transaction id number.
        abort : bool
            True if the transaction should be rolled back.

        Return
        ------
        success : bool
            True if the operation was successful.
        status : int
            Status code from the put operation.
        r_json : json or None
            json data from the put operation if status was 200.
        """
        cmd = 'ingest/trans/%d?abort=%d' % (transaction_id,abort)
        success, status, r_json = self._requestToIngest("PUT", cmd, {'auth_key': self._auth_key})
        if not success:
            print("ERROR ending transaction id=", transaction_id, "abort=", abort, "status=", status,
                  "r_json=", r_json)
        return success, status, r_json

    def getChunkTargetAddr(self, transaction_id, chunk_id):
        """ Get the host and port number of the ingest worker for this chunk.

        Parameters
        ----------
        transaction_id : int
            Ingest transaction id number.
        chunk_id : int
            Chunk id number.

        Return
        ------
        host : str
            Host name of the ingest worker.
        port : int
            Port number of the ingest worker.
        """
        cmd = 'ingest/chunk'
        jdata = {"transaction_id": transaction_id,"chunk": chunk_id,"auth_key": self._auth_key}
        success, status_code, r_json = self._requestToIngest("POST", cmd, jdata)
        if not success:
            print("ERROR failed to get chunk target address", jdata, " r_json=", r_json)
            raise RuntimeError('Transaction ' + transaction_id
                               + ' failed to get target address for chunk ' + chunk_id)
        host = r_json['location']['host']
        port = r_json['location']['port']
        return host, port

    def sendChunkToTarget(self, host, port, transaction_id, table, f_path):
        """ Send the file to the ingest worker.

        Parameters
        ----------
        host : str
            Host name for the ingest worker.
        port : int
            Port number for the ingest worker.
        transation_id : int
            Ingest transaction id number.
        table : str
            The name of the table that f_path data should be added to.
        f_path : str
            Full path to the file to ingest.

        Note
        ----
        This calls the external program 'qserv-replica-file-ingest' to
        actually send the file.
        """
        cmd = ('qserv-replica-file-ingest FILE ' + host + ' ' + str(port) + ' '
            + str(transaction_id) + ' ' + table + ' P ' + f_path + ' --verbose --columns-separator=TAB')
        print("cmd=", cmd)
        process = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        out_str = str(process.stdout)
        if process.returncode != 0:
            print("ERROR sendChunkToTarget cmd=", cmd, "out=", out_str)
            raise RuntimeError("ERROR sendChunkToTarget cmd=" + cmd + " out=" + str(out_str))
        return process.returncode, out_str


    def publishDatabase(self, db_name):
        """Once all the chunks have been ingested, call this to publish the database.

        Parameters
        ----------
        db_name : str
            Name of the database to be published.
        """
        success = False
        cmd = 'ingest/database/' + db_name
        success, status, r_json = self._requestToIngest("PUT", cmd,
                                                        {'auth_key': self._auth_key})
        if not success:
            print("ERROR publishing", db_name, "status=", status, "r_json=", r_json)
        return success, status, r_json

class IngestTransaction():
    """RAII object to make sure transactions are closed.
    Throws RunTimeError if transaction cannot be started or closed.
    self.abort needs to be set to False if the elements of the
    transaction are successful before __exit__ is called.
    """

    def __init__(self, data_ingest, db_name):
        self._data_ingest = data_ingest
        self._db_name = db_name
        self._id = -1
        self.abort = True

    def __repr__(self):
        out = 'data_ingest(' + str(self._data_ingest) + ") db=" + self._db_name + " id=" + str(self._id)
        return out

    def __enter__(self):
        success, id = self._data_ingest.startTransaction(self._db_name)
        if success:
            self._id = id
            print('Transaction started ', self._db_name, self._id)
        else:
            raise RuntimeError('Transaction failed to start ' + self._db_name + str(self._id))
        return self._id

    def __exit__(self, e_type, e_value, e_traceback):
        success = False
        if e_type:
            print("__exit__ exception=", e_type, "val=", e_value, "trace=", e_traceback)
            return False
        content = None
        status = -1
        if self._id > -1:
            success, status, content = self._data_ingest.endTransaction(self._id, abort=False)
        if not success:
            print("ERROR Transaction end failed ", self._db_name, self._id, status, content)
            raise RuntimeError('Transaction failed ' + self._db_name + " trans_id="+ str(self._id)
                                + " status=" + str(status) + " content=" + str(content))
        return True

