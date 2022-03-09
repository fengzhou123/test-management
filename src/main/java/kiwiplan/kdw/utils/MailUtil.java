package kiwiplan.kdw.utils;

import java.util.Properties;
import java.util.StringTokenizer;
import java.util.logging.Logger;

import javax.activation.DataHandler;
import javax.activation.DataSource;
import javax.activation.FileDataSource;

import javax.mail.*;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMessage;
import javax.mail.internet.MimeMultipart;

import kiwiplan.kdw.core.ConfigHelper;
import kiwiplan.kdw.core.LogHelper;

/**
 * Created by yplyf on 2014/11/25.
 */
public class MailUtil {
    public static Logger rootLogger = LogHelper.getRootLogger();

    public static void main(String args[]) {

//      MailUtil.sendJavaMail(
//              "FAILED: kdw-8.0 Automation",
//              "/data/kdw/automation/kdw-8.0/logs/compschema4freshupgrade_step.log",
//              "/data/kdw/automation/kdw-8.0/temp/logattachments.tar.gz",
//              "kyle.yu@kiwiplan.co.nz");
    }

    public static void sendJavaMail(String subject, String content, String attachment, String recipients,
                                    String mailContentType) {
        String hostname = ConfigHelper.getInstance().getConfig("System.ServerHostName");
        String username = ConfigHelper.getInstance().getConfig("KDWAUTOUSER");

        rootLogger.info("Sending mail : Subject is " + subject + " contentFilePath is " + " attachment is "
                        + attachment + " recipients are " + recipients);

        // Sender's email ID needs to be mentioned
        String from = username + "@" + hostname + ".kiwiplan.co.nz";

        // Get system properties
        Properties properties = System.getProperties();

        // Setup mail server
        properties.setProperty("mail.smtp.host", "nznotify.kiwiplan.co.nz");

        // Get the default Session object.
        Session session = Session.getDefaultInstance(properties);

        try {

            // Create a default MimeMessage object.
            MimeMessage message = new MimeMessage(session);

            // Set From: header field of the header.
            message.setFrom(new InternetAddress(from));

            // Set To: header field of the header.
            StringTokenizer st = new StringTokenizer(recipients, " ,;");

            while (st.hasMoreTokens()) {
                String recipient = st.nextToken();

                message.addRecipient(Message.RecipientType.TO, new InternetAddress(recipient));
            }

            // Set Subject: header field
            message.setSubject(subject);

            // Create the message part
            BodyPart messageBodyPart = new MimeBodyPart();

            // Fill the message
//          String content = FileUtils.readFileToString(new File(contentFilePath));
//          System.out.println(content);
            messageBodyPart.setContent(content, mailContentType);

            // Create a multipar message
            Multipart multipart = new MimeMultipart();

            // Set text message part
            multipart.addBodyPart(messageBodyPart);

            if ((attachment != null) &&!attachment.isEmpty()) {

                // Part two is attachment
                messageBodyPart = new MimeBodyPart();

                DataSource source = new FileDataSource(attachment);

                messageBodyPart.setDataHandler(new DataHandler(source));

                String attachmentname = attachment.substring(attachment.lastIndexOf("/") + 1, attachment.length());

                rootLogger.info("Attachment name is : " + attachmentname);
                messageBodyPart.setFileName(attachmentname);
                multipart.addBodyPart(messageBodyPart);
            }

            // Send the complete message parts
            message.setContent(multipart);

            // Send message
            Transport.send(message);

//          LogHelper.logTestcaseAll(LogHelper.LOGLEVEL_INFO, "Mail Sent successfully.");
            rootLogger.info("Mail Sent successfully.");
        } catch (MessagingException mex) {
            mex.printStackTrace();
        }
    }
}
