package es.cipfpbatoi.dao;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

// Patrón singleton
public class ConexionBD {

    private static final String JDBC_URL = "jdbc:mariadb://192.168.56.101:3306/empresa_ad_test";
 //   private static final String JDBC_URL = "jdbc:postgresql://192.168.56.101:5432/postgres?currentSchema=empresa_ad_test";
    
    private static Connection con = null;    

    public static Connection getConexion() throws SQLException {
        if (con == null) {
            Properties pc = new Properties();
            pc.put("user", "batoi");
            pc.put("password", "1234");
            con = DriverManager.getConnection(JDBC_URL, pc);
        }
        return con;
    }

    public static void cerrar() throws SQLException {
        if (con != null) {
            con.close();
            con = null;
        }
    }
    
    public static void iniciaTransaccion() throws SQLException {
    	con.setAutoCommit(false);
    }
    
    public static void confirmar() throws SQLException {
    	con.commit();
    	con.setAutoCommit(true);
    }
    
    public static void retrocede() throws SQLException {
    	con.rollback();
    	con.setAutoCommit(true);
    }

}
