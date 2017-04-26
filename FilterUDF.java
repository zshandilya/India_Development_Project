package pigUDF;

import java.io.IOException;

import org.apache.pig.FilterFunc;
import org.apache.pig.data.Tuple;

public class FilterUDF extends FilterFunc{

	@Override
	public Boolean exec(Tuple input) throws IOException {
		try {
			int val1 = (int) input.get(0);
			int val2 = (int) input.get(1);
			if(val2 >= (0.8*val1))
					return true;
			else
				return false;
			
		}
		catch(Exception e) {
			throw new IOException(e);
		}
	}

}
