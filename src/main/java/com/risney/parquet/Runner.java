package com.risney.parquet;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import com.risney.parquet.model.User;
import com.risney.parquet.service.UserService;

import lombok.extern.slf4j.Slf4j;


@Slf4j
@Component
public class Runner implements CommandLineRunner {

	private final UserService userService;
	
	
	public Runner(UserService userService) {
		this.userService = userService;
	}

	@Override
	public void run(String... args) throws Exception {
		log.info("creating parquet file ...");
		
		List<User> userList = new ArrayList<>();
		UUID uuid = UUID.randomUUID();
		User user = new User(uuid,"some.user@gmail.com",true);
		userList.add(user);
		userService.saveUsers(userList);
		
	}
}
