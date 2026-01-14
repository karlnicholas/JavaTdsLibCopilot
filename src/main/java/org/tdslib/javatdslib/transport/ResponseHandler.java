package org.tdslib.javatdslib.transport;

import org.tdslib.javatdslib.messages.Message;

import java.util.List;

public interface ResponseHandler {
    void onMessagesReceived(List<Message> messages);
}
