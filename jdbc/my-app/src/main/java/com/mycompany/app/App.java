package com.mycompany.app;

import java.sql.*;

public class App 
{
    public static void main( String[] args ) throws SQLException, InterruptedException
    {
        try (Connection conn = DriverManager.getConnection("jdbc:mariadb://127.0.0.1:4006/test", "maxuser", "maxpwd")) {
            System.out.println("Hello");
            
            try (Statement stmt = conn.createStatement()) {
                for (int i = 0; i < 700; i++){
                    try (ResultSet rs = stmt.executeQuery("SET autocommit=0")) {
                        rs.first();
                    }

                    try (ResultSet rs = stmt.executeQuery("SET autocommit=1")) {
                        rs.first();
                    }
                }

                for (int i = 0; i < 700; i++){
                    try (ResultSet rs = stmt.executeQuery("SELECT 1")) {
                        rs.first();
                        Thread.sleep(1500);
                        System.out.println(i);
                    }
                }
            }
        }
    }
}
