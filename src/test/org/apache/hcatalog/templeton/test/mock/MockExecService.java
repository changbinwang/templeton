package org.apache.hcatalog.templeton.test.mock;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.commons.exec.ExecuteException;
import org.apache.hcatalog.templeton.ExecBean;
import org.apache.hcatalog.templeton.ExecService;
import org.apache.hcatalog.templeton.NotAuthorizedException;

public class MockExecService implements ExecService {
	
	public ExecBean run(String user, String program, List<String> args,
            Map<String, String> env) {
		ExecBean bean = new ExecBean();
		bean.stdout = program;
		bean.stderr = args.toString();
		return bean;
	}

	@Override
	public ExecBean runUnlimited(String user, String program,
			List<String> args, Map<String, String> env)
			throws NotAuthorizedException, ExecuteException, IOException {
		ExecBean bean = new ExecBean();
		bean.stdout = program;
		bean.stderr = args.toString();
		return null;
	}
}
