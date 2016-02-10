from axolotl.state.prekeystore import PreKeyStore
from axolotl.state.prekeyrecord import PreKeyRecord


class PostgresPreKeyStore(PreKeyStore):
    def __init__(self, dbConn, table_prefix=''):
        """
        :type dbConn: Connection
        """
        self.dbConn = dbConn
        self.table_name = '{}_prekeys'.format(table_prefix)
        c = self.dbConn.cursor()
        c.execute("CREATE TABLE IF NOT EXISTS {} (_id serial PRIMARY KEY,"
                    "prekey_id BIGINT UNIQUE, sent_to_server BOOLEAN, record BYTEA);".format(self.table_name))

    def loadPreKey(self, preKeyId):
        q = "SELECT record FROM {} WHERE prekey_id = %s".format(self.table_name)

        cursor = self.dbConn.cursor()
        cursor.execute(q, (preKeyId,))

        result = cursor.fetchone()
        if not result:
            raise Exception("No such prekeyRecord!")

        return PreKeyRecord(serialized = bytes(result[0]))

    def loadPendingPreKeys(self):
        q = "SELECT record FROM {}".format(self.table_name)
        cursor = self.dbConn.cursor()
        cursor.execute(q)
        result = cursor.fetchall()

        return [PreKeyRecord(serialized=bytes(result[0])) for result in result]

    def storePreKey(self, preKeyId, preKeyRecord):
        #self.removePreKey(preKeyId)
        q = "INSERT INTO {} (prekey_id, record) VALUES(%s,%s)".format(self.table_name)
        cursor = self.dbConn.cursor()
        kk = preKeyRecord.serialize()
        cursor.execute(q, (preKeyId, preKeyRecord.serialize()))
        self.dbConn.commit()

    def containsPreKey(self, preKeyId):
        q = "SELECT record FROM {} WHERE prekey_id = %s".format(self.table_name)
        cursor = self.dbConn.cursor()
        cursor.execute(q, (preKeyId,))
        return cursor.fetchone() is not None

    def removePreKey(self, preKeyId):
        q = "DELETE FROM {} WHERE prekey_id = %s".format(self.table_name)
        cursor = self.dbConn.cursor()
        cursor.execute(q, (preKeyId,))
        self.dbConn.commit()
