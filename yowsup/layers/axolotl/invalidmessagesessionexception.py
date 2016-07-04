class InvalidMessageSessionException(Exception):
    def __init__(self, message, sender_username, sender_presence_name):
        self.sender_username = sender_username
        self.sender_presence_name = sender_presence_name
        super(InvalidMessageSessionException, self).__init__(message)


class InvalidMessageMediaException(Exception):
    def __init__(self, message, sender_username, sender_presence_name):
        self.sender_username = sender_username
        self.sender_presence_name = sender_presence_name
        super(InvalidMessageMediaException, self).__init__(message)
