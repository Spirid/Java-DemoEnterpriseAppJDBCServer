package EnterpriseJDBCServer;


import javax.annotation.*;
import javax.ejb.Stateful;

@Stateful
public class DBSessionBean implements DBSessionBeanRemote {

    JDBCConnectorToEmployer db = null;

    @Override
    public String[] getHistory(String name) {
        if (isLogin) {
            return db.getHistory(name);
        }
        return null;
    }

    @Override
    public String[] getHistory(int code) {
        if (isLogin) {
            return db.getHistory(code);
        }
        return null;
    }

    private boolean isLogin = false;

    @Override
    public boolean login(String user, String password) {
        isLogin = db.login(user, password);
        return isLogin;
    }

    @Override
    public boolean logout() {
        isLogin = false;
        return !isLogin;
    }

    @PostConstruct
    private void ConectToJDBC() {
        db = JDBCConnectorToEmployer.GetJDBCConnector();
        if (db.RegisterDriver()) {
            db.OpenConnection();
        }
    }

    @PreDestroy
    private void PostLogout() {
        db.CloseConnection();
    }

    public boolean GetServeraveleble() {

        return db.GetState();
    }
}
