package net.ninebow.demo.springboot.ooziejmsreceiver.jms;

import org.apache.oozie.client.event.Event;
import org.apache.oozie.client.event.jms.JMSHeaderConstants;
import org.apache.oozie.client.event.jms.JMSMessagingUtils;
import org.apache.oozie.client.event.message.JobMessage;
import org.apache.oozie.client.event.message.SLAMessage;
import org.springframework.jms.annotation.JmsListener;
import org.springframework.stereotype.Component;

import javax.jms.Message;
import java.util.Date;

@Component
public class JMSMessageReceiver {

    public JMSMessageReceiver() {
        System.out.println("JMSMessageReceiver Ready!");
    }

    @JmsListener(destination = "${spring.activemq.topic}", containerFactory = "topicListenerFactory")
    public void receiveTopicMessage(Message message) {

        try {
            // Print out Message by type
            if (Event.MessageType.SLA.name().equals(message.getStringProperty(JMSHeaderConstants.MESSAGE_TYPE))) {
                SLAMessage slaMessage = JMSMessagingUtils.getEventMessage(message);
                System.out.println("=== SLA Message ===");
                System.out.println("JMSDestination      : " + message.getJMSDestination());
                System.out.println("JMSTimestamp        : " + new Date(message.getJMSTimestamp()));
                System.out.println("JMSMessageID        : " + message.getJMSMessageID());
                System.out.println("appType             : " + slaMessage.getAppType());
                System.out.println("id                  : " + slaMessage.getId());
                System.out.println("parentId            : " + slaMessage.getParentId());
                System.out.println("nominalTime         : " + slaMessage.getNominalTime());
                System.out.println("expectedStartTime   : " + slaMessage.getExpectedStartTime());
                System.out.println("actualStartTime     : " + slaMessage.getActualStartTime());
                System.out.println("expectedEndTime     : " + slaMessage.getExpectedEndTime());
                System.out.println("actualEndTime       : " + slaMessage.getActualEndTime());
                System.out.println("expectedDuration    : " + slaMessage.getExpectedDuration());
                System.out.println("actualDuration      : " + slaMessage.getActualDuration());
                System.out.println("notificationMessage : " + slaMessage.getNotificationMessage());
                System.out.println("upstreamApps        : " + slaMessage.getUpstreamApps());
                System.out.println("user                : " + slaMessage.getUser());
                System.out.println("appName             : " + slaMessage.getAppName());
                System.out.println("eventStatus         : " + slaMessage.getEventStatus());
                System.out.println("slaStatus           : " + slaMessage.getSLAStatus());
                System.out.println();
            }
            else {
                JobMessage jobMessage = JMSMessagingUtils.getEventMessage(message);
                System.out.println("=== Job Message ===");
                System.out.println("JMSDestination      : " + message.getJMSDestination());
                System.out.println("JMSTimestamp        : " + new Date(message.getJMSTimestamp()));
                System.out.println("JMSMessageID        : " + message.getJMSMessageID());
                System.out.println("appType             : " + jobMessage.getAppType());
                System.out.println("id                  : " + jobMessage.getId());
                System.out.println("parentId            : " + jobMessage.getParentId());
                System.out.println("startTime           : " + jobMessage.getStartTime());
                System.out.println("endTime             : " + jobMessage.getEndTime());
                System.out.println("eventStatus         : " + jobMessage.getEventStatus());
                System.out.println("appName             : " + jobMessage.getAppName());
                System.out.println("user                : " + jobMessage.getUser());
                System.out.println();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
