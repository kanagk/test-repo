package com.reancloud.kanag.service;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Date;
import java.util.List;

import org.apache.log4j.Logger;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Service;
import org.springframework.util.FileCopyUtils;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.util.IOUtils;

@Service
public class RCService {
	
	private static String SQS_NAME="reancloud-sqs";
	private static String imgFileName="RC_logo";
	private static String imgFileExt=".jpg";
	private static String BUCKET_NAME="reancloud-kanag";
	private static String KEY="AKIAIYEALRXVVDFWI4SQ";
	private static String SECRET_KEY="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";
	private static BasicAWSCredentials awsCreds = new BasicAWSCredentials(KEY, SECRET_KEY);
	
	private static Logger logger= Logger.getLogger(RCService.class);

	/**
	 * Upload the image file to S3 and return the file id of the uploaded file
	 * @return Uploaded file Id
	 */
	public String uploadtoS3()  {
		
		String fileId=imgFileName+(new Date()).getTime()+imgFileExt;
		
		//Build the client
		AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
								.withRegion(Regions.US_EAST_1)
		                        .withCredentials(new AWSStaticCredentialsProvider(awsCreds))
		                        .build();
		
	    try {
	        	logger.info("Uploading file:"+fileId+" to bucket:"+BUCKET_NAME);
	        		
	        	 // Read Image File
	         InputStream inputStream = (new ClassPathResource("/"+imgFileName+imgFileExt)).getInputStream();	            
	         byte[] bytes = IOUtils.toByteArray(inputStream);
	         ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes);

	         //Set Metadata for S3
	         ObjectMetadata metadata = new ObjectMetadata();
	         metadata.setContentLength(bytes.length);
	            
	         //Upload file to S3   
	         s3Client.putObject(new PutObjectRequest(
	            		BUCKET_NAME, fileId, byteArrayInputStream,metadata));

	      } catch (AmazonServiceException ase) {
	        	 	logger.info("Caught AmazonServiceException:"+ase.getErrorMessage());
	      } catch (AmazonClientException ace) {
	        	 	logger.info("Caught AmazonServiceException:"+ace.getMessage());
	      }catch (IOException  io) {
	        	 	logger.info("Caught IO Exception: " + io.getMessage());
		  }
	      
	    //Return fileId
	      return fileId;
	}
	
	/**
	 * Send the file id of the uploaded file as a message to SQS 
	 * @param fileId - File Id 
	 * @return   
	 */
	public boolean sendMessage(String fileId)  {

		logger.info("Sending message:"+fileId+" to queue:"+SQS_NAME);
	
		//Create SQS Client
		AmazonSQS sqs = AmazonSQSClientBuilder.standard()
				.withRegion(Regions.US_EAST_1)
                .withCredentials(new AWSStaticCredentialsProvider(awsCreds))
				.build();
		
		//Send file id as a message to SQS
	    sqs.sendMessage(new SendMessageRequest(SQS_NAME, fileId));
	    
	    return true;
	}
	
	/**
	 * Read message from the queue and download the files from s3 using the fileid's contained in the message
	 * @return
	 */
	public boolean readMessageAndDownloadFile()  {

		logger.info("Reading from queue:"+SQS_NAME);
		
		//Create SQS Client
		AmazonSQS sqs = AmazonSQSClientBuilder.standard()
				.withRegion(Regions.US_EAST_1)
                .withCredentials(new AWSStaticCredentialsProvider(awsCreds))
				.build();
		
		//Receive messages from SQS
		ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(SQS_NAME);
        List<Message> messages = sqs.receiveMessage(receiveMessageRequest).getMessages();
        for (Message message : messages) {
        		String fileId=message.getBody();
        		logger.info("Received Message:"+fileId);
        		
        		//Download the file from S3 and save it in local file system
        		downloadFileFromS3(fileId);
        		
        		//Delete the Message
        		sqs.deleteMessage(SQS_NAME,message.getReceiptHandle());
        }

	    return true;
	}
	
	/**
	 * Method to do the actual download and save of the files from S3
	 * @param fileId
	 */
	private void downloadFileFromS3(String fileId)
	{
		try
		{
			//Create S3 Client
			AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
					.withRegion(Regions.US_EAST_1)
	                .withCredentials(new AWSStaticCredentialsProvider(awsCreds))
	                .build();
			
			//Read the file from S3 based on the file Id from the predefined bucket
			S3Object s3Object=s3Client.getObject(BUCKET_NAME,fileId);
			
			//Save file in local filesystem
			File targetFile = new File("/temp/"+fileId);			 
			FileCopyUtils.copy(IOUtils.toByteArray(s3Object.getObjectContent()), targetFile);
	
	    } catch (AmazonServiceException ase) {
	   	 	logger.info("Caught AmazonServiceException:"+ase.getErrorMessage());
	    } catch (AmazonClientException ace) {
	   	 	logger.info("Caught AmazonServiceException:"+ace.getMessage());
	    }catch (IOException  io) {
	   	 	logger.info("Caught IO Exception: " + io.getMessage());
	    }
		    
	}


}
