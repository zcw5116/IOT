package wlw;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import java.nio.charset.Charset;

/**
 * Unit test for simple App.
 */
public class AppTest

{
    public void convertByteArrayToString(Charset encoding) {

        byte[] byteArray = new byte[] {87, 79, 87, 46, 46, 46};

        String value = new String(byteArray, encoding);

        System.out.println(value);
    }

}
