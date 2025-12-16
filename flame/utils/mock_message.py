import datetime
import uuid
from typing import Optional
from mock_node_config import MockNodeConfig

class MockMessage:
    def __init__(self,
                 message: dict,
                 config: MockNodeConfig,
                 outgoing: bool,
                 message_number: Optional[int] = None,
                 category: Optional[str] = None,
                 recipients: Optional[list[str]] = None) -> None:
        """
        Message object to be sent or received by the message broker.
        :param message: dict body of the message to be sent or received, must not contain the field 'meta'
        :param config: the node configuration
        :param outgoing: boolean value specifying if the message is outgoing or incoming
        :param message_number: the message number
        :param category: the message category
        :param recipients: the list of recipients
        """
        if outgoing:
            if "meta" in message.keys():
                raise("Cannot use field 'meta' in message body. "
                                              "This field is reserved for meta data used by the message broker.")
            elif type(message_number) != int:
                raise(f"Specified outgoing message, but did not specify integer value for "
                                              f"message_number (received: {type(message_number)}).")
            elif type(category) != str:
                raise(f"Specified outgoing message, but did not specify string value for "
                                              f"category (received: {type(category)}).")
            elif (type(recipients) != list) or (any([type(recipient) != str for recipient in recipients])):
                if hasattr(recipients, '__iter__'):
                    raise(f"Specified outgoing message, but did not specify list of strings "
                                                  f"value for recipients (received: {type(recipients)} containing "
                                                  f"{set([type(recipient) for recipient in recipients])}).")
                else:
                    raise(f"Specified outgoing message, but did not specify list of strings "
                                                  f"value for recipients (received: {type(recipients)}).")
            self.recipients = recipients

        self.body = message
        self._update_meta_data(outgoing, config, category, message_number)

        if not outgoing:
            self.recipients = [self.body["meta"]["sender"]]

    def set_read(self) -> None:
        """
        Marks the message as read.
        :return:
        """
        self.body["meta"]["status"] = "read"

    def _update_meta_data(self,
                          outgoing: bool,
                          config: MockNodeConfig,
                          category: Optional[str] = None,
                          message_number: Optional[int] = None) -> None:
        """
        Adds meta data to the outgoing message or update it for incoming.
        :param outgoing:
        :param config:
        :param category:
        :param message_number:
        :return:
        """
        if outgoing:
            meta_data = {"type": "outgoing",
                         "category": category,
                         "id": f"{config.node_id}-{message_number}-{uuid.uuid4()}",
                         "akn_id": None,
                         "status": "unread",
                         "sender": config.node_id,
                         "created_at": str(datetime.datetime.now()),
                         "arrived_at": None,
                         "number": message_number}
            self.body["meta"] = meta_data
        else:
            self.body["meta"]["type"] = "incoming"
            if self.body["meta"]["akn_id"] is None:
                self.body["meta"]["akn_id"] = config.node_id
                self.body["meta"]["arrived_at"] = str(datetime.datetime.now())