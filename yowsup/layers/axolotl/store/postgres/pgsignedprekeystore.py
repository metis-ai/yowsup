from axolotl.state.signedprekeystore import SignedPreKeyStore
from axolotl.state.signedprekeyrecord import SignedPreKeyRecord
from axolotl.invalidkeyidexception import InvalidKeyIdException


class PostgresSignedPreKeyStore(SignedPreKeyStore):
    def __init__(self, dbConn, table_prefix=''):
        """
        :type dbConn: Connection
        """
        self.dbConn = dbConn
        self.table_name = '{}_signed_prekeys'.format(table_prefix)
        c = self.dbConn.cursor()
        c.execute("CREATE TABLE IF NOT EXISTS {} (_id serial PRIMARY KEY,"
                    "prekey_id BIGINT UNIQUE, timestamp BIGINT, record BYTEA);".format(self.table_name))


    def loadSignedPreKey(self, signedPreKeyId):
        q = "SELECT record FROM {} WHERE prekey_id = %s".format(self.table_name)

        cursor = self.dbConn.cursor()
        cursor.execute(q, (signedPreKeyId,))

        result = cursor.fetchone()
        if not result:
            raise InvalidKeyIdException("No such signedprekeyrecord! %s " % signedPreKeyId)

        return SignedPreKeyRecord(serialized=bytes(result[0]))

    def loadSignedPreKeys(self):
        q = "SELECT record FROM {}".format(self.table_name)

        cursor = self.dbConn.cursor()
        cursor.execute(q,)
        result = cursor.fetchall()
        results = []
        for row in result:
            results.append(SignedPreKeyRecord(serialized=bytes(row[0])))

        return results

    def storeSignedPreKey(self, signedPreKeyId, signedPreKeyRecord):
        #q = "DELETE FROM {} WHERE prekey_id = %s".format(self.table_name)
        #self.dbConn.cursor().execute(q, (signedPreKeyId,))
        #self.dbConn.commit()

        q = "INSERT INTO {} (prekey_id, record) VALUES(%s,%s)".format(self.table_name)
        cursor = self.dbConn.cursor()
        cursor.execute(q, (signedPreKeyId, signedPreKeyRecord.serialize()))
        self.dbConn.commit()

    def containsSignedPreKey(self, signedPreKeyId):
        q = "SELECT record FROM {} WHERE prekey_id = %s".format(self.table_name)
        cursor = self.dbConn.cursor()
        cursor.execute(q, (signedPreKeyId,))
        return cursor.fetchone() is not None

    def removeSignedPreKey(self, signedPreKeyId):
        q = "DELETE FROM {} WHERE prekey_id = %s".format(self.table_name)
        cursor = self.dbConn.cursor()
        cursor.execute(q, (signedPreKeyId,))
        self.dbConn.commit()
