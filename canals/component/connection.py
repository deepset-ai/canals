from dataclasses import dataclass
from typing import Optional

from canals.component.sockets import InputSocket, OutputSocket
from canals.type_utils import _type_name


@dataclass
class Connection:
    sender: Optional[str]
    sender_socket: Optional[OutputSocket]
    receiver: Optional[str]
    receiver_socket: Optional[InputSocket]

    def __hash__(self):
        return hash(
            "-".join(
                [
                    self.sender or "input",
                    self.sender_socket.name if self.sender_socket else "",
                    self.receiver or "output",
                    self.receiver_socket.name if self.receiver_socket else "",
                ]
            )
        )

    def __repr__(self):
        if self.sender and self.sender_socket:
            sender_repr = f"{self.sender}.{self.sender_socket.name} ({_type_name(self.sender_socket.type)})"
        else:
            sender_repr = "input needed"

        if self.receiver and self.receiver_socket:
            receiver_repr = f"({_type_name(self.receiver_socket.type)}) {self.receiver}.{self.receiver_socket.name}"
        else:
            receiver_repr = "output"

        return f"{sender_repr} --> {receiver_repr}"

    @property
    def is_mandatory(self) -> bool:
        """
        Returns True if the connection goes to a mandatory input socket, False otherwise
        """
        if self.receiver_socket:
            return self.receiver_socket.is_mandatory
        return False
