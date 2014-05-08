package bg.tusofia.cs.drmaa;

/**
 * Created by ivan on 5/7/14.
 */
public class SessionFactory extends org.ggf.drmaa.SessionFactory {
    @Override
    public org.ggf.drmaa.Session getSession() {
        return new Session();
    }
}
