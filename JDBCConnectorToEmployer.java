package EnterpriseJDBCServer;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public class JDBCConnectorToEmployer {

    // JDBC драйвера и URL базы данных
    static private final String JDBC_DRIVER = "org.apache.derby.jdbc.ClientDriver";
    static private final String SQL_SERVER_PORT = "1527";
    static private final String SQL_URL = "localhost";
    static private final String SQL_BASENAME = "demoDB";

    //  Учётные данные базы данных
    static private final String USR = "demoUser";
    static private final String PSW = "demo";

    static private Connection connection = null;
    static private Statement statement = null;

    private static volatile JDBCConnectorToEmployer instance = null;

    public static JDBCConnectorToEmployer GetJDBCConnector() {
        JDBCConnectorToEmployer localInstance = instance;
        if (localInstance == null) {
            synchronized (JDBCConnectorToEmployer.class) {
                localInstance = instance;
                if (localInstance == null) {
                    instance = localInstance = new JDBCConnectorToEmployer();
                }
            }
        }
        return localInstance;
    }

    private static boolean stateConnection = false;

    public boolean GetState() {
        return stateConnection;
    }

    synchronized boolean RegisterDriver() {
        try {
            Class.forName(JDBC_DRIVER);
            System.out.println("JDBC Derby Driver найден.");
            return true;
        } catch (ClassNotFoundException ex) {
            Logger.getLogger(JDBCConnectorToEmployer.class.getName()).log(Level.SEVERE, null, ex);
            System.err.println("JDBC Derby Driver не найден.");
            return false;
        }
    }

    synchronized boolean OpenConnection() {

        try {
            if (null == connection) {
                connection = DriverManager.getConnection("jdbc:derby://"
                        + SQL_URL + ":"
                        + SQL_SERVER_PORT + "/"
                        + SQL_BASENAME, USR, PSW);
            }
            System.out.println("Database connection complite.");

            statement = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            stateConnection = true;
            return true;
        } catch (SQLException ex) {
            Logger.getLogger(JDBCConnectorToEmployer.class.getName()).log(Level.SEVERE, null, ex);
            return false;
        }
    }

    synchronized void CloseConnection() {
        try {
            if (null != connection) {
                statement.close();
                connection.close();
            }
            connection = null;
        } catch (SQLException ex) {
            System.err.println("Connecton close error!");
            Logger.getLogger(JDBCConnectorToEmployer.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    synchronized private String[] getHistoryQuery(String query) {
        List<String> list = new ArrayList<>();
        Statement s = null;
        try {
            s = connection.createStatement();
            ResultSet rs = s.executeQuery(query);

            while (rs.next()) {
                StringBuilder strB = new StringBuilder();
                strB.append(rs.getString("Name")).append(":")
                        .append(rs.getString("Last name")).append(":")
                        .append(rs.getString("Position")).append(":")
                        .append(String.valueOf(rs.getInt("Manager"))).append(":")
                        .append(rs.getDate("Hire").toString()).append(":");
                Date date = rs.getDate("Dismiss");
                if (null != date) {
                    strB.append(date.toString());
                } else {
                    strB.append("Current");
                }
                strB.append(";");
                list.add(strB.toString());
            }

        } catch (SQLException ex) {
            System.err.println("SQL-query error!");
            Logger.getLogger(JDBCConnectorToEmployer.class.getName()).log(Level.SEVERE, null, ex);
        } finally {
            if (null != s) {
                try {
                    s.close();
                } catch (SQLException ex) {
                    System.err.println("SQL-query close error!");
                    Logger.getLogger(JDBCConnectorToEmployer.class.getName()).log(Level.SEVERE, null, ex);
                }
            }

        }
        if (!list.isEmpty()) {
            String[] res = (String[]) list.toArray(new String[list.size()]);
            return res;
        }
        return new String[0];

    }
    private final String TEMPLET_HISTORY_QUERY = "SELECT "
            + "emp.name as \"Name\", "
            + "emp.LAST_NAME   as \"Last name\", "
            + "emphis.position as \"Position\", "
            + "emphis.MANAGER  as \"Manager\", "
            + "emphis.hire     as \"Hire\", "
            + "emphis.DISMISS  as \"Dismiss\" "
            + "FROM EMPLOYEES emp INNER JOIN EMPLOYEEHISTORY emphis "
            + "ON emp.code = emphis.code "
            + "WHERE ";

    public String[] getHistory(String lastname) {
        String query = TEMPLET_HISTORY_QUERY + "emp.last_name = '" + lastname + "'";
        return getHistoryQuery(query);
    }

    public String[] getHistory(int code) {
        String query = TEMPLET_HISTORY_QUERY + "emp.CODE = " + code;
        return getHistoryQuery(query);
    }

    synchronized public boolean login(String user, String password) {
        try {
            return statement.executeQuery("SELECT CODE "
                    + "FROM EMPLOYEES "
                    + "WHERE (login='"
                    + user
                    + "') AND (psw='"
                    + password
                    + "')").next();

        } catch (SQLException ex) {
            Logger.getLogger(JDBCConnectorToEmployer.class.getName()).log(Level.SEVERE, null, ex);
        }
        return false;
    }

    public void CreateTables() {
        CreateTablesQ("EMPLOYEES", "create table EMPLOYEES ("
                + "CODE INTEGER not null primary key,"
                + "NAME VARCHAR(24) not null,"
                + "LAST_NAME VARCHAR(32) not null,"
                + "LOGIN VARCHAR(16) not null,"
                + "PSW VARCHAR(16))");
        CreateTablesQ("EMPLOYEEHISTORY", "create table EMPLOYEEHISTORY("
                + "ID INTEGER not null primary key GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1),"
                + "POSITION VARCHAR(24) not null,"
                + "MANAGER INTEGER,"
                + "HIRE DATE not null,"
                + "DISMISS DATE,"
                + "CODE INTEGER,"
                + "CONSTRAINT manager_more_then_null CHECK(manager > 0),"
                + "CONSTRAINT dismiss_more_then_or_equel_hire CHECK(DISMISS >= hire),"
                + "CONSTRAINT fk_EmpCode FOREIGN KEY (code) REFERENCES  EMPLOYEES (code))");

        InsertToEmployees(1, "Vasilii", "Pupkin", "qwerty", "qwerty");
        InsertToEmployees(2, "Aleks", "Smit", "zxc", "zxc");
        InsertToEmployees(3, "Mishel", "Bone", "qwerty", "123");
        InsertToEmployees(4, "Slava", "Jaic", "D3C0D3", "D3C0D3");
        InsertToEmployees(5, "Nicol", "Longhard", "12345", "12345");

        InsertToEmployeesHistory(1,"Director", 1, Date.valueOf("2001-02-24"), null, 1);
        InsertToEmployeesHistory(2,"Asistent", 1, Date.valueOf("2001-02-24"), Date.valueOf("2014-03-24"), 2);
        InsertToEmployeesHistory(3,"Engineer", 1, Date.valueOf("2001-02-26"), null, 3);
        InsertToEmployeesHistory(4,"Coder", 1, Date.valueOf("2016-01-01"), null, 4);
        InsertToEmployeesHistory(5,"Manager", 1, Date.valueOf("2001-05-24"), null, 5);

    }

    synchronized void InsertToEmployees(int code, String name, String LastName, String Login, String PSW) {
        Statement s = null;
        try {

            s = connection.createStatement();
            String q = "INSERT INTO EMPLOYEES (CODE, NAME, LAST_NAME, LOGIN, PSW)  "
                    + "VALUES ("
                    + code + ",'"
                    + name + "','"
                    + LastName + "','"
                    + Login + "','"
                    + PSW + "')";
            s.executeUpdate(q);
        } catch (SQLException ex) {
            Logger.getLogger(JDBCConnectorToEmployer.class.getName()).log(Level.SEVERE, null, ex);
        } finally {
            if (null != s) {
                try {
                    s.close();
                } catch (SQLException ex) {
                    Logger.getLogger(JDBCConnectorToEmployer.class.getName()).log(Level.SEVERE, null, ex);
                }
            }

        }
    }

    synchronized void InsertToEmployeesHistory(int id, String position, int manager, Date hire, Date dismiss, int code) {
        Statement s = null;
        try {
            s = connection.createStatement();
            String dismissDate = "NULL";
            if (null != dismiss) {
                dismissDate = "'" + dismiss + "'";
            }
            String q = "INSERT INTO EMPLOYEEHISTORY (ID, POSITION, MANAGER, HIRE, DISMISS, CODE) "
                    + "VALUES ("
                    + id + ",'"
                    + position + "',"
                    + manager + ",'"
                    + hire + "',"
                    + dismissDate + ","
                    + code + ")";
            s.executeUpdate(q);
            id++;
        } catch (SQLException ex) {
            Logger.getLogger(JDBCConnectorToEmployer.class.getName()).log(Level.SEVERE, null, ex);
        } finally {
            if (null != s) {
                try {
                    s.close();
                } catch (SQLException ex) {
                    Logger.getLogger(JDBCConnectorToEmployer.class.getName()).log(Level.SEVERE, null, ex);
                }
            }

        }
    }

    synchronized private void CreateTablesQ(String table, String q) {
        if (null == connection) {
            return;
        }
        Statement s = null;
        try {

            s = connection.createStatement();

            s.executeUpdate(q);
            System.out.println("Table " + table + " successfully created.");
        } catch (SQLException ex) {
            Logger.getLogger(JDBCConnectorToEmployer.class.getName()).log(Level.SEVERE, null, ex);
            System.out.println("Table " + table + " already exists.");
        } finally {
            try {
                if (null != s) {
                    s.close();
                }
            } catch (SQLException ex) {
                Logger.getLogger(JDBCConnectorToEmployer.class.getName()).log(Level.SEVERE, null, ex);
                System.err.println("Query closing error!");
            }
        }
    }

}
