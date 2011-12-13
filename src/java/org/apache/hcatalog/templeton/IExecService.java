package org.apache.hcatalog.templeton;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.commons.exec.ExecuteException;

public interface IExecService {

	public ExecBean run(String user, String program, List<String> args,
            Map<String, String> env)
            		throws NotAuthorizedException, BusyException, ExecuteException, IOException;
	
	public ExecBean runUnlimited(String user, String program, List<String> args,
            Map<String, String> env)
            		throws NotAuthorizedException, ExecuteException, IOException;
}
