package org.apache.hcatalog.templeton;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.security.UserGroupInformation;

public class UgiFactory {
    private static ConcurrentHashMap<String, UserGroupInformation> userUgiMap =
            new ConcurrentHashMap<String, UserGroupInformation>();
    
    static UserGroupInformation getUgi(String user) throws IOException{
        UserGroupInformation ugi = userUgiMap.get(user);
        if(ugi == null){
            //create new ugi and add to map
            final UserGroupInformation newUgi = 
                    UserGroupInformation.createProxyUser(user,
                            UserGroupInformation.getLoginUser());

            //if another thread adds an entry before the check in this one
            // the one created here will not be added.
            userUgiMap.putIfAbsent(user, newUgi);

            //use the UGI object that got added
            return userUgiMap.get(user);
            
        }
        return ugi;
    }
    
    
}
