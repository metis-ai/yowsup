from axolotl.state.sessionstore import SessionStore
from axolotl.state.sessionrecord import SessionRecord


class PostgresSessionStore(SessionStore):
    def __init__(self, dbConn, table_prefix=''):
        """
        :type dbConn: Connection
        """
        self.dbConn = dbConn
        self.table_name = '{}_sessions'.format(table_prefix)
        c = self.dbConn.cursor()
        c.execute("CREATE TABLE IF NOT EXISTS {} (_id serial PRIMARY KEY,"
                    "recipient_id TEXT, device_id BIGINT, record BYTEA, timestamp BIGINT);".format(self.table_name))
        # TODO: add unique for recipient_id+device_id


    def loadSession(self, recipientId, deviceId):
        q = "SELECT record FROM {} WHERE recipient_id = %s AND device_id = %s".format(self.table_name)
        c = self.dbConn.cursor()
        c.execute(q, (recipientId, deviceId))
        result = c.fetchone()

        if result:
            return SessionRecord(serialized=bytes(result[0]))
        else:
            return SessionRecord()

    def getSubDeviceSessions(self, recipientId):
        q = "SELECT device_id from {} WHERE recipient_id = %s".format(self.table_name)
        c = self.dbConn.cursor()
        c.execute(q, (recipientId,))
        result = c.fetchall()

        deviceIds = [r[0] for r in result]
        return deviceIds

    def storeSession(self, recipientId, deviceId, sessionRecord):
        self.deleteSession(recipientId, deviceId)

        q = "INSERT INTO {}(recipient_id, device_id, record) VALUES(%s,%s,%s)".format(self.table_name)
        c = self.dbConn.cursor()
        c.execute(q, (recipientId, deviceId, sessionRecord.serialize()))
        self.dbConn.commit()

    def containsSession(self, recipientId, deviceId):
        q = "SELECT record FROM {} WHERE recipient_id = %s AND device_id = %s".format(self.table_name)
        c = self.dbConn.cursor()
        c.execute(q, (recipientId, deviceId))
        result = c.fetchone()

        return result is not None

    def deleteSession(self, recipientId, deviceId):
        q = "DELETE FROM {} WHERE recipient_id = %s AND device_id = %s".format(self.table_name)
        self.dbConn.cursor().execute(q, (recipientId, deviceId))
        self.dbConn.commit()

    def deleteAllSessions(self, recipientId):
        q = "DELETE FROM {} WHERE recipient_id = %s".format(self.table_name)
        self.dbConn.cursor().execute(q, (recipientId,))
        self.dbConn.commit()
