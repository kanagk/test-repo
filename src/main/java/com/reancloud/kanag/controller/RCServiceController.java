package com.reancloud.kanag.controller;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.reancloud.kanag.model.RCServiceResponse;
import com.reancloud.kanag.service.RCService;

@RestController
@EnableAutoConfiguration
public class RCServiceController {
	
	private static Logger logger= Logger.getLogger(RCServiceController.class);

	@Autowired
	RCService service;
	
	@RequestMapping(value="/", method = RequestMethod.POST)
	public @ResponseBody  RCServiceResponse ingestFile(){
		
		//Upload file to S3
		String fileId=service.uploadtoS3();
		
		//Send file id as a message to SQS
		service.sendMessage(fileId);
		
		//Build Response
		RCServiceResponse response=new RCServiceResponse();
		response.setFileId(fileId);
		
		return response;
	}
	
	@RequestMapping(value="/", method = RequestMethod.GET)
	public @ResponseBody  String processMessagesAndDownloadFile(){
		
		logger.info("Processing Messages from SQS");
		
		//Process messages in SQS.
		service.readMessageAndDownloadFile();
		
		logger.info("Completed Processing Messages from SQS");

		return "Message Processing Completed";
	}
	
	
}
