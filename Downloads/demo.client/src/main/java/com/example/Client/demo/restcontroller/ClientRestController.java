package com.example.Client.demo.restcontroller;

import org.springframework.http.MediaType;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class ClientRestController {
	@RequestMapping(value = "/employees", produces = MediaType.APPLICATION_XML_VALUE, method = RequestMethod.GET)
	public String getAllEmployeesXML(Model model)
	{
	    model.addAttribute("employees", getEmployeesCollection());
	    return "xmlTemplate";
	}

	private Object getEmployeesCollection() {
		// TODO Auto-generated method stub
		return null;
	}
}
