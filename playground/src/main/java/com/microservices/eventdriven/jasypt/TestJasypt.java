package com.microservices.eventdriven.jasypt;

import org.jasypt.encryption.pbe.StandardPBEStringEncryptor;
import org.jasypt.iv.RandomIvGenerator;

public class TestJasypt {

  public static void main(String[] args) {
    final String jaspytSecretKey = "SecretKey!!";

    StandardPBEStringEncryptor standardPBEStringEncryptor = new StandardPBEStringEncryptor();
    standardPBEStringEncryptor.setPassword(jaspytSecretKey);
    standardPBEStringEncryptor.setAlgorithm("PBEWITHHMACSHA512ANDAES_256");
    standardPBEStringEncryptor.setIvGenerator(new RandomIvGenerator());

    final String encryptedPwd = standardPBEStringEncryptor.encrypt("TestPassword");
    final String decryptedPwd = standardPBEStringEncryptor.decrypt(encryptedPwd);

    System.out.println("Encrypted Password: " + encryptedPwd);
    System.out.println("Decrypted Password: " + decryptedPwd);
  }
}