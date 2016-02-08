from axolotl.state.identitykeystore import IdentityKeyStore
from axolotl.ecc.curve import Curve
from axolotl.identitykey import IdentityKey
from axolotl.util.keyhelper import KeyHelper
from axolotl.identitykeypair import IdentityKeyPair
from axolotl.ecc.djbec import *


class PostgresIdentityKeyStore(IdentityKeyStore):
    def __init__(self, dbConn, table_prefix=''):
        """
        :type dbConn: Connection
        """
        self.dbConn = dbConn
        self.table_name = '{}_identities'.format(table_prefix)
        c = self.dbConn.cursor()
        c.execute("CREATE TABLE IF NOT EXISTS {} (_id serial PRIMARY KEY,"
                   "recipient_id BIGINT UNIQUE,"
                   "registration_id BIGINT, public_key BYTEA, private_key BYTEA,"
                   "next_prekey_id BIGINT, timestamp BIGINT);".format(self.table_name))

        #identityKeyPairKeys = Curve.generateKeyPair()
        #self.identityKeyPair = IdentityKeyPair(IdentityKey(identityKeyPairKeys.getPublicKey()),
        #                                       identityKeyPairKeys.getPrivateKey())
        # self.localRegistrationId = KeyHelper.generateRegistrationId()

    def getIdentityKeyPair(self):
        q = "SELECT public_key, private_key FROM {} WHERE recipient_id = -1".format(self.table_name)
        c = self.dbConn.cursor()
        c.execute(q)
        result = c.fetchone()

        publicKey, privateKey = result
        return IdentityKeyPair(IdentityKey(DjbECPublicKey(bytes(publicKey)[1:])), DjbECPrivateKey(bytes(privateKey)))

    def getLocalRegistrationId(self):
        q = "SELECT registration_id FROM {} WHERE recipient_id = -1".format(self.table_name)
        c = self.dbConn.cursor()
        c.execute(q)
        result = c.fetchone()
        return result[0] if result else None


    def storeLocalData(self, registrationId, identityKeyPair):
        q = "INSERT INTO {}(recipient_id, registration_id, public_key, private_key) VALUES(-1, %s, %s, %s)".format(self.table_name)
        c = self.dbConn.cursor()
        c.execute(q, (registrationId, identityKeyPair.getPublicKey().getPublicKey().serialize(),
                      identityKeyPair.getPrivateKey().serialize()))

        self.dbConn.commit()

    def saveIdentity(self, recipientId, identityKey):
        q = "DELETE FROM {} WHERE recipient_id=%s".format(self.table_name)
        self.dbConn.cursor().execute(q, (recipientId,))
        self.dbConn.commit()


        q = "INSERT INTO {} (recipient_id, public_key) VALUES(%s, %s)".format(self.table_name)
        c = self.dbConn.cursor()
        c.execute(q, (recipientId, identityKey.getPublicKey().serialize()))
        self.dbConn.commit()

    def isTrustedIdentity(self, recipientId, identityKey):
        q = "SELECT public_key from {} WHERE recipient_id = %s".format(self.table_name)
        c = self.dbConn.cursor()
        c.execute(q, (recipientId,))
        result = c.fetchone()
        if not result:
            return True
        return result[0] == identityKey.getPublicKey().serialize()