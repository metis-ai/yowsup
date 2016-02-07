from axolotl.state.signedprekeystore import SignedPreKeyStore
from axolotl.state.signedprekeyrecord import SignedPreKeyRecord
from axolotl.invalidkeyidexception import InvalidKeyIdException


class PostgresSignedPreKeyStore(SignedPreKeyStore):
    def __init__(self, dbConn):
        """
        :type dbConn: Connection
        """
        self.dbConn = dbConn
        c = self.dbConn.cursor()
        c.execute("CREATE TABLE IF NOT EXISTS signed_prekeys (_id serial PRIMARY KEY,"
                    "prekey_id BIGINT UNIQUE, timestamp BIGINT, record BYTEA);")


    def loadSignedPreKey(self, signedPreKeyId):
        q = "SELECT record FROM signed_prekeys WHERE prekey_id = %s"

        cursor = self.dbConn.cursor()
        cursor.execute(q, (signedPreKeyId,))

        result = cursor.fetchone()
        if not result:
            raise InvalidKeyIdException("No such signedprekeyrecord! %s " % signedPreKeyId)

        return SignedPreKeyRecord(serialized=bytes(result[0]))

    def loadSignedPreKeys(self):
        q = "SELECT record FROM signed_prekeys"

        cursor = self.dbConn.cursor()
        cursor.execute(q,)
        result = cursor.fetchall()
        results = []
        for row in result:
            results.append(SignedPreKeyRecord(serialized=bytes(row[0])))

        return results

    def storeSignedPreKey(self, signedPreKeyId, signedPreKeyRecord):
        #q = "DELETE FROM signed_prekeys WHERE prekey_id = %s"
        #self.dbConn.cursor().execute(q, (signedPreKeyId,))
        #self.dbConn.commit()

        q = "INSERT INTO signed_prekeys (prekey_id, record) VALUES(%s,%s)"
        cursor = self.dbConn.cursor()
        cursor.execute(q, (signedPreKeyId, signedPreKeyRecord.serialize()))
        self.dbConn.commit()

    def containsSignedPreKey(self, signedPreKeyId):
        q = "SELECT record FROM signed_prekeys WHERE prekey_id = %s"
        cursor = self.dbConn.cursor()
        cursor.execute(q, (signedPreKeyId,))
        return cursor.fetchone() is not None

    def removeSignedPreKey(self, signedPreKeyId):
        q = "DELETE FROM signed_prekeys WHERE prekey_id = %s"
        cursor = self.dbConn.cursor()
        cursor.execute(q, (signedPreKeyId,))
        self.dbConn.commit()
